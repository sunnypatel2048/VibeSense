from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
from typing import List, Dict
from datetime import datetime

load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def fetch_comments(video_id: str) -> List[Dict]:
    """
    Fetches comments for a video ID with pagination.
    Returns a list of dicts: {'comment_id': str, 'text': str, 'published_at': datetime, 'metrics': dict}
    """
    comments = []
    next_page_token = None

    while True:
        response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            order="time",
            maxResults=100,
            pageToken=next_page_token
        ).execute()

        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            published_at_str = snippet["publishedAt"]
            published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
            comments.append({
                "comment_id": item["id"],
                "text": snippet["textOriginal"],
                "published_at": snippet["publishedAt"],
                "metrics": {
                    "like_count": snippet.get("likeCount", 0)
                }
            })

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return comments