from pydantic import BaseModel, EmailStr, validator
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple
from sqlalchemy import Column, String, Float, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from uuid import uuid4

Base = declarative_base()

# DB Tables (Schema)
class MonitoringJobDB(Base):
    __tablename__ = "monitoring_jobs"
    job_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    post_id = Column(String, nullable=False)
    post_title = Column(String)
    email = Column(String, nullable=False)
    intervals_seconds = Column(Float, nullable=False)  # e.g., 14400 for 4 hours
    total_duration_seconds = Column(Float, nullable=False)  # e.g., 86400 for 1 day
    is_scheduled = Column(Boolean, default=False)
    last_fetched_at = Column(DateTime, default=None)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))

class IntervalResultDB(Base):
    __tablename__ = "interval_results"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("monitoring_jobs.job_id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    avg_sentiment = Column(Float)
    confidence_interval = Column(ARRAY(Float))  # [low, high]
    summary = Column(String)
    raw_comments = Column(JSON)  # Store list of CommentData JSON for history

# Pydantic Models (for API/Validation)
class UserInput(BaseModel):
    full_name: str
    post_url: str
    email: EmailStr
    duration: str  # e.g., "1 day 4 hour"

class MonitoringJob(BaseModel):
    job_id: str  # UUID as string
    post_id: str
    post_title: str
    intervals: float
    total_duration: float
    email: EmailStr

class CommentData(BaseModel):
    comment_id: str
    text: str
    published_at: datetime
    metrics: Dict[str, int]  # e.g., {'like_count': int}

class AnalysisOutput(BaseModel):
    text: str
    sentiment: str  # "POSITIVE", "NEGATIVE", "NEUTRAL"
    confidence: float  # 0.0-1.0
    summary: str  # Condensed text

class Aggregate(BaseModel):
    interval_sentiment: float  # Avg score
    overall_sentiment: float
    ci: Tuple[float, float]  # Confidence interval
    summary: str  # Combined