from .app import celery_app
from src.models import MonitoringJobDB, CommentData
from src.ingestion_service.youtube_fetcher import fetch_comments
from src.ingestion_service.preprocessor import preprocess_text
import pika
import json
from dotenv import load_dotenv
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import structlog
from datetime import datetime, timedelta, timezone

load_dotenv()
DB_URL = os.getenv("DB_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
logger = structlog.get_logger()

@celery_app.task(name='src.ingestion_service.tasks.process_job_task')
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
def process_job(job_data: dict):
    logger.info("Processing job", job_id=job_data['job_id'])
    try:
        # Get DB session
        db = SessionLocal()

        # Query job for last_fetched_at
        job = db.query(MonitoringJobDB).filter(MonitoringJobDB.job_id == job_data['job_id']).first()
        last_fetched_at = job.last_fetched_at if job and job.last_fetched_at else None

        # Fetch all comments
        all_comments = fetch_comments(job_data['post_id'], last_fetched_at)

        # Filter new comments (published_at > last_fetched_at)
        if last_fetched_at:
            new_comments = [c for c in all_comments if c['published_at'] > last_fetched_at]
        else:
            new_comments = all_comments

        if not new_comments:
            logger.info("No new comments", job_id=job_data['job_id'])
            return

        # Preprocess comments
        preprocessed = [preprocess_text(c['text']) for c in new_comments]
        batches = [preprocessed[i:i + 50] for i in range(0, len(preprocessed), 50)]

        # Metadata for tracibility
        interval_timestamp = max(c['published_at'] for c in new_comments)
        metadata = {
            'job_id': job_data['job_id'],
            'interval_timestamp': interval_timestamp.isoformat()
        }

        # Publish batches to RabbitMQ
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue='analysis_queue', durable=True)
        for batch in batches:
            payload = { **metadata, 'batch': batch }
            channel.basic_publish(
                exchange='',
                routing_key='analysis_queue',
                body=json.dumps(payload)
            )
        connection.close()

        new_last_fetched_at = max(c['published_at'] for c in new_comments) if new_comments else datetime.now(datetime.timezone.utc)
        job.last_fetched_at = new_last_fetched_at
        db.commit()
        db.close()

        logger.info("Data ingested and published", job_id=job_data['job_id'])
    except Exception as e:
        logger.error("Ingestion failed", job_id=job_data['job_id'], error=str(e))
        raise

@celery_app.task(name='src.ingestion_service.tasks.refresh_schedule_task')
def refresh_schedule_task():
    """Refresh Celery Beat schedule."""
    logger.info("Refreshing Celery Beat schedule")
    celery_app.conf.beat_schedule.update(dynamic_schedule())


def dynamic_schedule():
    """Generate dynamic schedule based on MonitoringJobDB."""
    db = SessionLocal()
    schedule = {}
    try:
        jobs = db.query(MonitoringJobDB).filter(MonitoringJobDB.is_scheduled == False).all()
        for job in jobs:
            logger.info("Scheduling job", job_id=job.job_id)
            # Only schedule if job is still active (within total_duration)
            expiration_time_naive = job.created_at + timedelta(seconds=job.total_duration_seconds)
            expiration_time_aware = expiration_time_naive.replace(tzinfo=timezone.utc)
            if expiration_time_aware > datetime.now(timezone.utc):
                interval_seconds = job.intervals_seconds
                schedule[f"ingest-job-{job.job_id}"] = {
                    "task": "src.ingestion_service.tasks.process_job_task",
                    "schedule": interval_seconds,
                    "args": ({"job_id": str(job.job_id), "post_id": job.post_id},)
                }
                job.is_scheduled = True
                db.commit()
                logger.info("Job scheduled", job_id=job.job_id, interval_seconds=interval_seconds)
    finally:
        db.close()
        return schedule