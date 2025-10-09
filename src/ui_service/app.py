import streamlit as st
from dotenv import load_dotenv
import os
import pika
import json
from uuid import uuid4
from datetime import timedelta
import re
import requests
from src.models import UserInput, MonitoringJob
from pydantic import ValidationError
from src.utils import parse_youtube_video_id

load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

st.set_page_config(page_title="VibeSense", layout="centered", initial_sidebar_state="collapsed")
st.markdown("""
    <style>
    .main { background-color: #f0f4f8; }
    .stButton>button { background-color: #4CAF50; color: white; }         
    </style>
    """, unsafe_allow_html=True)

st.title("VibeSense")
st.subheader("Track sentiment trends on social media posts over time.")

with st.form(key="input_form"):
    full_name = st.text_input("Full Name", max_chars=50)
    post_url = st.text_input("YouTube Post URL", help="e.g., https://www.youtube.com/watch?v=VIDEO_ID")
    email = st.text_input("Email Address", max_chars=100)
    duration = st.selectbox("Monitoring Duration", ["1 day 4 hour", "3 days 8 hour", "1 week 24 hour"])
    submit_button = st.form_submit_button(label="Start Monitoring")

if submit_button:
    with st.spinner("Validating and Queuing Job..."):
        try:
            input_data = UserInput(
                full_name=full_name,
                post_url=post_url,
                email=email,
                duration=duration
            )

            post_id = parse_youtube_video_id(input_data.post_url)

            # Fetch Video Title for Confirmation
            api_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet&id={post_id}&key={YOUTUBE_API_KEY}"
            response = requests.get(api_url).json()
            if 'items' not in response or not response['items']:
                raise ValueError("Invalid video ID or private video")
            title = response['items'][0]['snippet']['title']
            st.success(f"Video Confirmed: {title}")

            # Parse Duration
            match = re.match(r'(\d+) day (\d+) hour', duration)
            if not match:
                raise ValueError("Invalid duration format")
            total_days = int(match.group(1))
            interval_hours = int(match.group(2))
            intervals = timedelta(hours=interval_hours).total_seconds()
            total_duration = timedelta(days=total_days).total_seconds()

            # Create Job
            job = MonitoringJob(
                job_id=str(uuid4()),
                post_id=post_id,
                post_title=title,
                intervals=intervals,
                total_duration=total_duration,
                email=email
            )

            # Queue Job
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            channel = connection.channel()
            channel.queue_declare(queue="monitoring_jobs", durable=True)
            channel.basic_publish(exchange='', routing_key="monitoring_jobs", body=json.dumps(job.model_dump()))
            connection.close()
            
            st.success("Monitoring job queued successfully! You'll receive email updates.")

        except (ValueError, ValidationError) as e:
            print(f"Validation error: {str(e)}")
            st.error("Input validation failed. Please check your entries.")
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            st.error("An unexpected error occurred. Please try again later.")