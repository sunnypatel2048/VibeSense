# VibeSense: Social Media Sentiment Monitoring as a Service

## Overview

VibeSense is a AI-based service that monitors sentiments in social media comments. It allows users to input a post URL, email, and monitoring duration via a simple UI, then fetches comments at intervals, analyzes them with pre-trained models, aggregates insights, and sends email reports. Built with microservices for modularity and scalability.

Key goals: Automate reputation tracking for marketing teams and small businesses, providing interval-based summaries without manual effort.

## Features

- User-friendly Streamlit UI for inputs (name, post URL, email, duration e.g., "1 day 4 hour").
- Interval-based comment fetching using the APIs like YouTube Data API v3, etc.
- AI sentiment analysis and summarization using Hugging Face models.
- Aggregates per-interval and overall insights with confidence intervals, stored in PostgreSQL.
- Automated email notifications with formatted reports.

## Technology Stack

- Frontend: Streamlit
- Backend: Python, FastAPI (microservices)
- AI: Hugging Face Transformers (DistilBERT for sentiment, BART for summarization)
- Messaging: RabbitMQ
- Database: PostgreSQL (via SQLAlchemy)
- Scheduling: Celery/APScheduler
- Other: Docker for containerization, Consul for service discovery

## Setup Instructions

1. Install Dependencies:

    ```bash
    pip intsall -r requirements.txt
    ```

2. Set up environment variables (in .env):

    ```text
    YOUTUBE_API_KEY=your_api_key_here
    DB_URL=postgresql://user:pass@localhost:5432/vibesense
    RABBITMQ_URL=amqp://guest:guest@localhost:5672/
    SMTP_SERVER=smtp.gmail.com
    SMTP_PORT=587
    SMTP_USER=your_email@gmail.com
    SMTP_PASS=your_app_password
    ```

3. Start infrastructure (DB, RabbitMQ, Redis for Celery):

    ```bash
    docker-compose up -d
    ```

4. Initialize DB schema:

    ```bash
    python src/db.py
    ```

## Running the Project

- Start services individually (for dev):

    ```text
    # In separate terminals:
    celery -A src.tasks worker --loglevel=info  # For scheduling
    uvicorn src.ui_service.app:app --reload
    uvicorn src.ingestion_service.app:app --reload
    uvicorn src.nlp_service.app:app --reload
    uvicorn src.aggregation_service.app:app --reload
    uvicorn src.notification_service.app:app --reload
    ```

- Or use Docker Compose for full stack.