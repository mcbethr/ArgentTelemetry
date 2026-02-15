#!/usr/bin/env python3
"""
ArgentTelemetry - Export YouTube comments to Excel (.xlsx)

Outputs one Excel file per video with two sheets:
  1) "Telemetry Data"  - comment rows (includes channel_name column)
  2) "Comment data"    - channel/video metadata

Batch mode also produces:
  Master-YYYY-MM-DD_HHMMSS.xlsx
  (Concatenation of all Telemetry Data sheets)

Usage:
  export YT_API_KEY="YOUR_KEY_HERE"

  Single:
  python3 Argent.py "https://www.youtube.com/watch?v=VIDEOID" --include-replies

  Batch:
  python3 Argent.py --video-file videos.txt --out-dir exports --include-replies
"""

import os
import re
import argparse
from typing import Dict, Any, Iterator, Optional, List
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter


# -------------------------
# Helpers: Input Parsing
# -------------------------

def read_video_list(path: str) -> List[str]:
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
        raise ValueError(f"Expected string video ID, got {type(url_or_id)}")

    url_or_id = url_or_id.strip()

    if re.fullmatch(r"[A-Za-z0-9_-]{11}", url_or_id):
        return url_or_id

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

    raise ValueError(f"Could not extract video ID from: {url_or_id}")


def sanitize_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '', name).strip()


# -------------------------
# YouTube API
# -------------------------

def youtube_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def get_video_metadata(yt, video_id: str) -> Dict[str, str]:
    resp = yt.videos().list(part="snippet", id=video_id).execute()
    if not resp.get("items"):
        raise ValueError(f"Video not found: {video_id}")

    snip = resp["items"][0]["snippet"]

    return {
        "video_id": video_id,
        "video_title": snip.get("title", ""),
        "upload_date": snip.get("publishedAt", ""),
        "channel_id": snip.get("channelId", ""),
        "channel_name": snip.get("channelTitle", ""),
        "video_link": f"https://www.youtube.com/watch?v={video_id}",
    }


def get_channel_handle_like(yt, channel_id: str) -> str:
    if not channel_id:
        return ""

    resp = yt.channels().list(part="snippet", id=channel_id).execute()
    if not resp.get("items"):
        return ""

    return resp["items"][0]["snippet"].get("customUrl", "") or ""


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
            order=order,
            textFormat="plainText",
            pageToken=page_token,
        )
        resp = req.execute()

        for item in resp.get("items", []):
            thr_snip = item["snippet"]
            top = thr_snip["topLevelComment"]["snippet"]

            yield {
                "comment_id": item["snippet"]["topLevelComment"]["id"],
                "parent_id": "",
                "author": top.get("authorDisplayName"),
                "author_channel_url": top.get("authorChannelUrl"),
                "published_at": top.get("publishedAt"),
                "updated_at": top.get("updatedAt"),
                "like_count": top.get("likeCount", 0),
                "is_pinned": thr_snip.get("isPinned"),
                "text": top.get("textDisplay", ""),
            }

            if include_replies and "replies" in item:
                for r in item["replies"]["comments"]:
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
# Excel Helpers
# -------------------------

def autosize_columns(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 80)


def export_video_to_excel(
    yt,
    video_input: str,
    out_dir: str,
    order: str,
    include_replies: bool,
) -> str:

    video_id = extract_video_id(video_input)
    meta = get_video_metadata(yt, video_id)
    channel_handle = get_channel_handle_like(yt, meta["channel_id"])

    safe_channel = sanitize_filename(meta["channel_name"]) or "UnknownChannel"
    os.makedirs(out_dir, exist_ok=True)

    out_xlsx = os.path.join(
        out_dir,
        f"{safe_channel}-{meta['channel_id']}-{video_id}.xlsx"
    )

    wb = Workbook()

    # Telemetry Sheet
    ws = wb.active
    ws.title = "Telemetry Data"
    ws.append([
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
    for row in iter_comment_threads(yt, video_id, order, include_replies):
        ws.append([
            meta["channel_name"],
            video_id,
            row["comment_id"],
            row["parent_id"],
            row["author"],
            row["author_channel_url"],
            row["published_at"],
            row["updated_at"],
            row["like_count"],
            row["is_pinned"],
            row["text"],
        ])
        count += 1

    # Metadata Sheet
    ws_meta = wb.create_sheet("Comment data")
    ws_meta.append(["Channel name", "Channel handle", "Video link", "Video Title", "Upload date"])
    ws_meta.append([
        meta["channel_name"],
        channel_handle,
        meta["video_link"],
        meta["video_title"],
        meta["upload_date"],
    ])

    autosize_columns(ws)
    autosize_columns(ws_meta)

    wb.save(out_xlsx)
    print(f"Wrote {count} comments -> {out_xlsx}")
    return out_xlsx


def create_master_workbook(excel_paths: List[str], out_dir: str) -> str:
    tz = ZoneInfo("America/New_York")
    stamp = datetime.now(tz).strftime("%Y-%m-%d_%H%M%S")
    out_path = os.path.join(out_dir, f"Master-{stamp}.xlsx")

    master_wb = Workbook()
    master_ws = master_wb.active
    master_ws.title = "Telemetry Data"

    wrote_header = False

    for p in excel_paths:
        wb = load_workbook(p, read_only=True, data_only=True)
        if "Telemetry Data" not in wb.sheetnames:
            wb.close()
            continue

        ws = wb["Telemetry Data"]
        rows = ws.iter_rows(values_only=True)

        try:
            header = next(rows)
        except StopIteration:
            wb.close()
            continue

        if not wrote_header:
            master_ws.append(list(header))
            wrote_header = True

        for r in rows:
            master_ws.append(list(r))

        wb.close()

    master_wb.save(out_path)
    print(f"Master workbook created -> {out_path}")
    return out_path


# -------------------------
# Main
# -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("video", nargs="?", help="Single video URL/ID")
    ap.add_argument("--video-file", help="Text file with video URLs/IDs")
    ap.add_argument("--out-dir", default="exports", help="Output directory")
    ap.add_argument("--order", choices=["time", "relevance"], default="time")
    ap.add_argument("--include-replies", action="store_true")
    args = ap.parse_args()

    load_dotenv()
    api_key = os.getenv("YT_API_KEY")
    if not api_key:
        raise SystemExit("Set env var YT_API_KEY first.")

    yt = youtube_client(api_key)

    # Batch mode
    if args.video_file:
        videos = read_video_list(args.video_file)
        if not videos:
            raise SystemExit("No valid videos found.")

        outputs = []
        for v in videos:
            try:
                out_path = export_video_to_excel(
                    yt, v, args.out_dir, args.order, args.include_replies
                )
                outputs.append(out_path)
            except Exception as e:
                print(f"[!] Failed: {v} -> {e}")

        if outputs:
            create_master_workbook(outputs, args.out_dir)
        return

    # Single mode
    if not args.video:
        raise SystemExit("Provide a video or --video-file.")

    export_video_to_excel(
        yt, args.video, ".", args.order, args.include_replies
    )


if __name__ == "__main__":
    main()