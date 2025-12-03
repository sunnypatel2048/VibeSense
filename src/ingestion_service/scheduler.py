import os
from celery import Celery
from redbeat.schedulers import RedBeatScheduler
from dotenv import load_dotenv

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL")

def configure_celery_beat(app: Celery):
    """Configure Celery Beat with RedBeat for dynamic scheduling."""
    
    app.conf.beat_scheduler = RedBeatScheduler
    app.conf.redbeat_redis_url = REDIS_URL
    app.conf.beat_max_loop_interval = 60

    app.conf.beat_schedule = {
        "refresh-schedule": {
            "task": "src.ingestion_service.tasks.refresh_dynamic_schedule",
            "schedule": 60,
            "args": ()
        }
    }