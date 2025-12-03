from celery import Celery

def configure_celery_beat(app: Celery):
    """Configure Celery Beat with dynamic schedule."""
    app.conf.beat_schedule = {
        "refresh-schedule": {
            "task": "src.ingestion_service.tasks.refresh_schedule_task",
            "schedule": 60,
        }
    }
    #app.conf.beat_schedule_filename = "celerybeat-schedule"
    #app.conf.beat_max_loop_interval = 60  # Check for schedule updates every minute

    # Refresh schedule is is adding jobs from db, then iprocess_job_task is scheduled, but it never runs?