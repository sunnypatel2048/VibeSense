from datetime import datetime, timezone
from fastapi import FastAPI
from dotenv import load_dotenv
import os
import pika
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import structlog
import smtplib
from email.mime.text import MIMEText
from jinja2 import Template
from src.models import Aggregate, Base, MonitoringJobDB

load_dotenv()
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
DB_URL = os.getenv("DB_URL")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = SMTP_USER

app = FastAPI(title="Notification Service")
logger = structlog.get_logger()

# DB Setup
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)

# Email Template (Jinja2)
email_template = Template("""
Hi {{ full_name }},

Exciting update! We've analyzed the latest comments on your video up to {{ interval_timestamp }}. Here's what the buzz is saying:

**Interval Highlights** 🌟:
- Average Sentiment: {{ interval_sentiment }} (Confidence Range: {{ ci[0] }} - {{ ci[1] }})
- Key Summary: {{ summary }}

**Overall Trends So Far** 📈:
- Average Sentiment: {{ overall_sentiment }} (Confidence Range: {{ ci[0] }} - {{ ci[1] }})
- Big Picture: {{ overall_summary }}

Act on these insights—reply to comments or tweak your content to boost engagement!

Stay tuned for more,
The VibeSense Team
P.S. Questions? Reply to this email.
""")

# API Endpoint for Manual Testing
@app.post("/notify/{job_id}")
async def notify_manual(job_id: str, aggregate: Aggregate):
    """Manual email trigger for testing."""
    send_email("test_post_id", aggregate, datetime.now(timezone.utc), "spatel48@umbc.edu", "Test Summary")
    return {"status": "Email sent"}

# Queue Consumer (Run in Worker Process)
def run_consumer():
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60),
           retry=retry_if_exception_type(pika.exceptions.AMQPError))
    def consume():
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue="notification_queue", durable=True)

        def callback(ch, method, properties, body):
            db = SessionLocal()
            try:
                data = json.loads(body)
                aggregate = Aggregate(**data['aggregate'])
                metadata = {k: v for k, v in data.items() if k != 'aggregate'}

                job = db.query(MonitoringJobDB).filter(MonitoringJobDB.job_id == metadata['job_id']).first()
                if not job:
                    raise ValueError("Job not found in DB")

                email = job.email
                post_title = job.post_title
                interval_timestamp = metadata.get('interval_timestamp')
                overall_summary = "Overall summary placeholder"  # Compute or fetch from DB if needed

                send_email(post_title, aggregate, interval_timestamp, email, overall_summary)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info("Email sent", job_id=metadata.get('job_id'))
            except Exception as e:
                logger.error("Notification failed", error=str(e))
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        channel.basic_consume(queue="notification_queue", on_message_callback=callback)
        logger.info("Notification Consumer started")
        channel.start_consuming()

    consume()

def send_email(post_title: str, aggregate: Aggregate, interval_timestamp: str, to_email: str, overall_summary: str):
    """Send formatted email."""
    msg_content = email_template.render(
        post_title=post_title,
        interval_timestamp=interval_timestamp,
        interval_sentiment=aggregate.interval_sentiment,
        ci=aggregate.ci,
        summary=aggregate.summary,
        overall_sentiment=aggregate.overall_sentiment,
        overall_summary=overall_summary
    )
    msg = MIMEText(msg_content)
    msg['From'] = FROM_EMAIL
    msg['To'] = to_email
    msg['Subject'] = f"🚀 VibeSense Alert: Fresh Insights for Your Video - {post_title}"

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

if __name__ == "__main__":
    # For running the consumer worker: python src/notification_service/app.py
    run_consumer()