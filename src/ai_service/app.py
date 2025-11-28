from fastapi import FastAPI
from dotenv import load_dotenv
import os
import pika
import json
from transformers import pipeline
from typing import List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import structlog
from src.models import AnalysisOutput

load_dotenv()
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

app = FastAPI(title="AI Service")
logger = structlog.get_logger()

# Load Models
sentiment_pipe = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
summary_pipe = pipeline("summarization", model="facebook/bart-large-cnn")

# API Endpoint for Sync Testing
@app.post("/analyze/", response_model=List[AnalysisOutput])
async def analyze_text(texts: List[str]):
    """Process text batches for sentiment and summary."""
    return process_batch(texts)

# Queue Consumer (Run in Worker Process)
def run_consumer():
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60),
           retry=retry_if_exception_type(pika.exceptions.AMQPError))
    def consume():
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue="analysis_queue", durable=True)
        channel.queue_declare(queue="aggregation_queue", durable=True)

        def callback(ch, method, properties, body):
            try:
                data = json.loads(body)
                batch = data['batch']  # List of texts
                metadata = {k: v for k, v in data.items() if k != 'batch'}
                results = process_batch(batch)
                payload = {**metadata, 'results': [r.model_dump() for r in results]}
                ch.basic_publish(exchange='', routing_key="aggregation_queue", body=json.dumps(payload))
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info("Batch processed and published", metadata=metadata)
            except Exception as e:
                logger.error("Processing failed", error=str(e))
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        channel.basic_consume(queue="analysis_queue", on_message_callback=callback)
        logger.info("NLP Consumer started")
        channel.start_consuming()

    consume()

def process_batch(texts: List[str]) -> List[AnalysisOutput]:
    """Process batch with AI models."""
    try:
        sent_results = sentiment_pipe(texts, batch_size=32, truncation=True)
        sum_results = summary_pipe(texts, max_length=100, min_length=30, batch_size=32, truncation=True)
        outputs = []
        for sent, sum in zip(sent_results, sum_results):
            #logger.log(sent)
            output = AnalysisOutput(
                text="",
                sentiment=sent['label'],
                confidence=sent['score'],
                summary=sum['summary_text']
            )
            outputs.append(output)
        return outputs
    except Exception as e:
        logger.error("AI processing failed", error=str(e))
        raise

if __name__ == "__main__":
    # For running the consumer worker: python3 src/ai_service/app.py
    run_consumer()