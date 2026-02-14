#!/usr/bin/env python3
"""
Export YouTube comments to CSV:
- author (display name)
- publishedAt
- likeCount
- isPinned (top-level comments only)

Requires:
  pip install google-api-python-client python-dateutil

Usage:
  export YT_API_KEY="Key Here"
  python Argent.py "https://www.youtube.com/watch?v=2erRc2mimhE" --out comments.csv --include-replies
"""

import os
import re
import csv
import argparse
from typing import Dict, Any, Iterator, Optional, List
from dotenv import load_dotenv, find_dotenv
load_dotenv()
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def extract_video_id(url_or_id: str) -> str:
    # Accept raw video ID
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", url_or_id):
        return url_or_id

    # Common URL formats
    patterns = [
        r"v=([A-Za-z0-9_-]{11})",
        r"youtu\.be/([A-Za-z0-9_-]{11})",
        r"shorts/([A-Za-z0-9_-]{11})",
        r"embed/([A-Za-z0-9_-]{11})",
    ]
    for p in patterns:
        m = re.search(p, url_or_id)
        if m:
            return m.group(1)

    raise ValueError(f"Could not extract a video id from: {url_or_id}")


def youtube_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def iter_comment_threads(
    yt,
    video_id: str,
    order: str = "time",
    include_replies: bool = False,
) -> Iterator[Dict[str, Any]]:
    """
    Yields dict rows for each top-level comment (and optionally replies).
    """
    page_token: Optional[str] = None

    while True:
        req = yt.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=100,
            order=order,          # "time" or "relevance"
            textFormat="plainText",
            pageToken=page_token,
        )
        resp = req.execute()

        for item in resp.get("items", []):
            thr_snip = item["snippet"]
            top = thr_snip["topLevelComment"]["snippet"]

            # Pinned is available on the commentThread snippet in many cases
            is_pinned = thr_snip.get("isPinned")

            yield {
                "comment_id": item["snippet"]["topLevelComment"]["id"],
                "parent_id": "",
                "author": top.get("authorDisplayName"),
                "author_channel_url": top.get("authorChannelUrl"),
                "published_at": top.get("publishedAt"),
                "updated_at": top.get("updatedAt"),
                "like_count": top.get("likeCount", 0),
                "is_pinned": is_pinned,
                "text": top.get("textDisplay", ""),
            }

            if include_replies and "replies" in item:
                replies = item["replies"].get("comments", [])
                for r in replies:
                    r_snip = r["snippet"]
                    yield {
                        "comment_id": r["id"],
                        "parent_id": item["snippet"]["topLevelComment"]["id"],
                        "author": r_snip.get("authorDisplayName"),
                        "author_channel_url": r_snip.get("authorChannelUrl"),
                        "published_at": r_snip.get("publishedAt"),
                        "updated_at": r_snip.get("updatedAt"),
                        "like_count": r_snip.get("likeCount", 0),
                        "is_pinned": False,
                        "text": r_snip.get("textDisplay", ""),
                    }

        page_token = resp.get("nextPageToken")
        if not page_token:
            break


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("video", help="YouTube video URL or 11-char video id")
    ap.add_argument("--out", default="comments.csv", help="Output CSV filename")
    ap.add_argument("--order", choices=["time", "relevance"], default="time", help="API sort order")
    ap.add_argument("--include-replies", action="store_true", help="Also export replies")
    args = ap.parse_args()

    load_dotenv() #load the environment variables.

    api_key = os.getenv('YT_API_KEY')
    if not api_key:
        raise SystemExit("Set env var YT_API_KEY to your YouTube Data API key first.")

    video_id = extract_video_id(args.video)
    yt = youtube_client(api_key)

    fieldnames = [
        "comment_id",
        "parent_id",
        "author",
        "author_channel_url",
        "published_at",
        "updated_at",
        "like_count",
        "is_pinned",
        "text",
    ]

    try:
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()

            count = 0
            for row in iter_comment_threads(yt, video_id, order=args.order, include_replies=args.include_replies):
                w.writerow(row)
                count += 1

        print(f"Wrote {count} comments to {args.out}")

    except HttpError as e:
        # Common errors: commentsDisabled, quotaExceeded, forbidden (private video)
        raise SystemExit(f"YouTube API error: {e}")


if __name__ == "__main__":
    main()
