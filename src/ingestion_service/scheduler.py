from celery import Celery
from celery.schedules import crontab
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import MonitoringJobDB
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
REDIS_URL = os.getenv("REDIS_URL")

engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def configure_celery_beat(app: Celery):
    """Configure Celery Beat to schedule jobs based on MonitoringJobDB.intervals."""
    def dynamic_schedule():
        db = SessionLocal()
        schedule = {}
        try:
            jobs = db.query(MonitoringJobDB).all()
            for job in jobs:
                # Only schedule if job is still active (within total_duration)
                if job.created_at + timedelta(seconds=job.total_duration_seconds) > datetime.now(datetime.timezone.utc):
                    interval_seconds = job.intervals_seconds
                    schedule[f"ingest-job-{job.job_id}"] = {
                        "task": "src.ingestion_service.tasks.process_job_task",
                        "schedule": interval_seconds,
                        "args": ({"job_id": str(job.job_id), "post_id": job.post_id},)
                    }
        finally:
            db.close()
        return schedule

    app.conf.beat_schedule = dynamic_schedule()
    app.conf.beat_schedule_filename = "celerybeat-schedule"