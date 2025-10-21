import pytest
from src.ingestion_service.youtube_fetcher import fetch_comments
from src.ingestion_service.preprocessor import preprocess_text

def test_fetch_comments(mocker):
    mocker.patch('googleapiclient.discovery.build')  # Mock API
    # Add assertions for mock response

def test_preprocess_text():
    assert preprocess_text("Hello, world! https://example.com") == "hello world"