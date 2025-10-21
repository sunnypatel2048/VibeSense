from fastapi import FastAPI
from dotenv import load_dotenv
import os
from celery import Celery
import structlog
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import MonitoringJobDB

load_dotenv()
DB_URL = os.getenv("DB_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
REDIS_URL = os.getenv("REDIS_URL")


app = FastAPI(title="Data Ingestion Service")
logger = structlog.get_logger()

# Celery setup
celery_app = Celery(
    'ingestion',
    broker=RABBITMQ_URL,
    backend=REDIS_URL,
    include=['src.ingestion_service.tasks']
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Database setup
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# API Endpoints
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/ingest/{job_id}")
async def ingest_manual(job_id: str):
    """Manual trigger for testing."""
    from .tasks import process_job
    process_job.delay({"job_id": job_id, "post_id": "dQw4w9WgXcQ"}) # Mock post_id
    return {"status": f"Job {job_id} queued for ingestion."}




## how will we handle data ingestion triggers after every intervals?