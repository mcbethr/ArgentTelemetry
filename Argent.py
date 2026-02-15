#!/usr/bin/env python3
"""
ArgentTelemetry - Export YouTube comments to Excel (.xlsx)

Outputs one Excel file per video with two sheets:
  1) "Telemetry Data"  - comment rows (includes channel_name column)
  2) "Comment data"    - channel/video metadata

Usage:
  export YT_API_KEY="YOUR_KEY_HERE"
  python3 Argent.py "https://www.youtube.com/watch?v=VIDEOID" --include-replies
  python3 Argent.py --video-file videos.txt --out-dir exports --include-replies

videos.txt format:
  - One URL/ID per line
  - Lines starting with # are ignored
  - Inline comments supported: URL # note
"""

import os
import re
import argparse
from typing import Dict, Any, Iterator, Optional, List

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from openpyxl import Workbook
from openpyxl.utils import get_column_letter


# -------------------------
# Helpers: input parsing
# -------------------------

def read_video_list(path: str) -> List[str]:
    """
    Reads newline-separated video URLs/IDs.

    Rules:
      - Blank lines ignored
      - Lines whose first non-whitespace char is '#' are ignored
      - Inline comments supported: anything after a '#' is ignored
        Example: https://youtu.be/abc123xyz00  # note
    """
    vids: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#"):
                continue
            if "#" in line:
                line = line.split("#", 1)[0].strip()
            if line:
                vids.append(line)
    return vids


def extract_video_id(url_or_id: str) -> str:
    if not isinstance(url_or_id, str):
        raise ValueError(f"Expected video URL/ID string, got: {type(url_or_id)}")

    url_or_id = url_or_id.strip()

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


def sanitize_filename(name: str) -> str:
    # macOS-safe filename (strip reserved chars)
    return re.sub(r'[<>:"/\\|?*]', '', name).strip()


# -------------------------
# YouTube API
# -------------------------

def youtube_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def get_video_metadata(yt, video_id: str) -> Dict[str, str]:
    resp = yt.videos().list(part="snippet", id=video_id).execute()
    if not resp.get("items"):
        raise ValueError(f"Video not found or not accessible: {video_id}")

    snip = resp["items"][0]["snippet"]
    channel_id = snip.get("channelId", "")
    channel_name = snip.get("channelTitle", "")

    return {
        "video_id": video_id,
        "video_title": snip.get("title", ""),
        "upload_date": snip.get("publishedAt", ""),
        "channel_id": channel_id,
        "channel_name": channel_name,
        "video_link": f"https://www.youtube.com/watch?v={video_id}",
    }


def get_channel_handle_like(yt, channel_id: str) -> str:
    """
    Best-effort channel 'handle' field.
    The API commonly provides 'customUrl' which is sometimes "@handle" and sometimes not.
    """
    if not channel_id:
        return ""

    resp = yt.channels().list(part="snippet", id=channel_id).execute()
    if not resp.get("items"):
        return ""

    snip = resp["items"][0]["snippet"]
    return snip.get("customUrl", "") or ""


def iter_comment_threads(
    yt,
    video_id: str,
    order: str = "time",
    include_replies: bool = False,
) -> Iterator[Dict[str, Any]]:
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


# -------------------------
# Excel output
# -------------------------

def autosize_columns(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 80)


def export_video_to_excel(
    yt,
    video_input: str,
    out_dir: str,
    order: str,
    include_replies: bool,
) -> str:
    """
    Exports one video's comments to an .xlsx and returns output path.
    """
    video_id = extract_video_id(video_input)

    meta = get_video_metadata(yt, video_id)
    channel_handle = get_channel_handle_like(yt, meta["channel_id"])

    channel_name = meta["channel_name"]
    channel_id = meta["channel_id"]
    safe_channel = sanitize_filename(channel_name) or "UnknownChannel"

    os.makedirs(out_dir, exist_ok=True)
    out_xlsx = os.path.join(out_dir, f"{safe_channel}-{channel_id}-{video_id}.xlsx")

    wb = Workbook()

    # Sheet 1: Telemetry Data
    ws_telemetry = wb.active
    ws_telemetry.title = "Telemetry Data"
    ws_telemetry.append([
        "channel_name",
        "video_id",
        "comment_id",
        "parent_id",
        "author",
        "author_channel_url",
        "published_at",
        "updated_at",
        "like_count",
        "is_pinned",
        "text",
    ])

    count = 0
    for row in iter_comment_threads(yt, video_id, order=order, include_replies=include_replies):
        ws_telemetry.append([
            channel_name,
            video_id,
            row.get("comment_id", ""),
            row.get("parent_id", ""),
            row.get("author", ""),
            row.get("author_channel_url", ""),
            row.get("published_at", ""),
            row.get("updated_at", ""),
            row.get("like_count", 0),
            row.get("is_pinned", False),
            row.get("text", ""),
        ])
        count += 1

    # Sheet 2: Comment data (metadata)
    ws_meta = wb.create_sheet("Comment data")
    ws_meta.append(["Channel name", "Channel handle", "Video link", "Video Title", "Upload date"])
    ws_meta.append([
        meta["channel_name"],
        channel_handle,
        meta["video_link"],
        meta["video_title"],
        meta["upload_date"],
    ])

    autosize_columns(ws_telemetry)
    autosize_columns(ws_meta)

    wb.save(out_xlsx)
    print(f"Wrote {count} comments -> {out_xlsx}")
    return out_xlsx


# -------------------------
# Main
# -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("video", nargs="?", help="YouTube video URL or 11-char video id (single mode)")
    ap.add_argument("--video-file", help="Text file with YouTube video URLs/IDs, one per line (batch mode)")
    ap.add_argument("--out-dir", default="exports", help="Output directory for batch exports (or single if you want)")
    ap.add_argument("--order", choices=["time", "relevance"], default="time", help="API sort order")
    ap.add_argument("--include-replies", action="store_true", help="Also export replies")
    args = ap.parse_args()

    load_dotenv()
    api_key = os.getenv("YT_API_KEY")
    if not api_key:
        raise SystemExit("Set env var YT_API_KEY to your YouTube Data API key first.")

    yt = youtube_client(api_key)

    # Batch mode
    if args.video_file:
        videos = read_video_list(args.video_file)
        if not videos:
            raise SystemExit(f"No videos found in {args.video_file}")

        ok, failed = 0, 0
        for v in videos:
            try:
                export_video_to_excel(
                    yt=yt,
                    video_input=v,
                    out_dir=args.out_dir,
                    order=args.order,
                    include_replies=args.include_replies,
                )
                ok += 1
            except HttpError as e:
                print(f"[!] YouTube API error for {v}: {e}")
                failed += 1
            except Exception as e:
                print(f"[!] Failed for {v}: {e}")
                failed += 1

        print(f"\nDone. Success: {ok}, Failed: {failed}")
        return

    # Single mode
    if not args.video:
        raise SystemExit("Provide a single video URL/ID or use --video-file <path>.")

    export_video_to_excel(
        yt=yt,
        video_input=args.video,
        out_dir=".",  # single mode writes to current directory
        order=args.order,
        include_replies=args.include_replies,
    )


if __name__ == "__main__":
    main()