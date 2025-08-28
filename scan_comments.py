#!/usr/bin/env python3
import os
import csv
import time
import base64
import logging
import requests
from typing import Dict, List, Optional
from urllib.parse import urlparse

# --- Config / Keys (YouTube key always required) ---
YOUTUBE_API_KEY = "AIzaSyBG9p3EOvsfvl6K7QMyF9PP4okVl2CNbgE"
if not YOUTUBE_API_KEY:
    raise SystemExit("Please set YOUTUBE_API_KEY")

# --- Endpoints ---
YT_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"

# --- Prompt for Gemini ---
PROMPT = (
    "Identify whether the image contains an animal or an illegally traded wilidlife product. "
    "Reply strictly just with a yes or no"
)

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("yt_thumbs_gemini")

# -------------------- Helpers --------------------
def _get(url: str, params: Dict, max_retries: int = 5, backoff: float = 1.7) -> Dict:
    attempt = 0
    while True:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        attempt += 1
        if attempt > max_retries:
            raise RuntimeError(f"GET {url} failed: {r.status_code} {r.text[:400]}")
        time.sleep(backoff ** attempt)

def guess_mime(url: str) -> str:
    p = urlparse(url).path.lower()
    if p.endswith(".png"): return "image/png"
    if p.endswith(".webp"): return "image/webp"
    return "image/jpeg"  # default for i.ytimg.com

def search_videos(keyword: str, n: int = 200) -> List[Dict]:
    items: List[Dict] = []
    page_token: Optional[str] = None
    while len(items) < n:
        to_fetch = min(50, n - len(items))
        params = {
            "key": YOUTUBE_API_KEY,
            "part": "snippet",
            "type": "video",
            "q": keyword,
            "maxResults": to_fetch,
            "order": "date",
            "safeSearch": "none",
        }
        if page_token:
            params["pageToken"] = page_token
        data = _get(YT_SEARCH_URL, params)
        page_items = data.get("items", [])
        items.extend(page_items)
        page_token = data.get("nextPageToken")
        if not page_token or not page_items:
            break
    return items

def pick_thumbnail_url(snippet: Dict, video_id: str) -> Optional[str]:
    thumbs = (snippet or {}).get("thumbnails", {}) or {}
    for key in ("maxres", "standard", "high", "medium", "default"):
        if key in thumbs and "url" in thumbs[key]:
            return thumbs[key]["url"]
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

def fetch_bytes(url: str, max_retries: int = 4, backoff: float = 1.7) -> bytes:
    attempt = 0
    while True:
        r = requests.get(url, timeout=30)
        if r.status_code == 200 and r.content:
            return r.content
        attempt += 1
        if attempt > max_retries:
            raise RuntimeError(f"GET {url} failed: {r.status_code} {r.text[:200]}")
        time.sleep(backoff ** attempt)

def call_gemini_on_image(image_bytes: bytes, mime_type: str, model: str, api_key: str) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload = {
        "contents": [{
            "role": "user",
            "parts": [
                {"text": PROMPT},
                {"inline_data": {"mime_type": mime_type, "data": base64.b64encode(image_bytes).decode("ascii")}}
            ],
        }]
    }
    r = requests.post(url, params={"key": api_key}, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini error {r.status_code}: {r.text[:500]}")
    data = r.json()
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        return ""

def normalize_yes_no(s: str) -> str:
    s = (s or "").strip().lower()
    if s.startswith("y"): return "yes"
    if s.startswith("n"): return "no"
    return "no"

# -------------------- Main --------------------
def main():
    import argparse
    ap = argparse.ArgumentParser(
        description="YouTube search → optionally send thumbnails to Gemini 1.5 Pro for yes/no wildlife detection."
    )
    ap.add_argument("keyword", help="YouTube search keyword")
    ap.add_argument("--max_results", type=int, default=200, help="Max videos to fetch")
    ap.add_argument("--csv", default="yt_thumbnail_hits.csv", help="Output CSV filename")
    ap.add_argument("--sleep", type=float, default=0.2, help="Delay between Gemini calls (seconds)")
    # NEW: true/false flag; default false
    ap.add_argument("--analyze_thumbnails", choices=["true", "false"], default="false",
                    help="If 'true', call Gemini on each thumbnail; else skip (default: false)")
    # Optional Gemini config (only needed when analyze_thumbnails=true)
    ap.add_argument("--gemini_model", default= "gemini-1.5-pro-latest")
    ap.add_argument("--gemini_api_key", default="AIzaSyA2BwX7quE1Mf0_eA4KmVOFqeq0rd_F5So")
    args = ap.parse_args()

    do_hits = (args.analyze_thumbnails == "true")
    if do_hits and not args.gemini_api_key:
        raise SystemExit("analyze_thumbnails=true but no GEMINI_API_KEY/GOOGLE_API_KEY provided.")

    log.info(f"Searching YouTube for '{args.keyword}' (up to {args.max_results})…")
    items = search_videos(args.keyword, n=args.max_results)
    videos = []
    for it in items:
        vid = (it.get("id") or {}).get("videoId")
        sn = it.get("snippet") or {}
        if not vid:
            continue
        videos.append({
            "video_id": vid,
            "title": sn.get("title", ""),
            "channel_title": sn.get("channelTitle", ""),
            "published_at": sn.get("publishedAt", ""),
            "thumbnail_url": pick_thumbnail_url(sn, vid),
        })

    log.info(f"Found {len(videos)} videos. Thumbnail analysis: {'ON' if do_hits else 'OFF'}")

    results = []
    yes_hits = 0

    for i, v in enumerate(videos, 1):
        vid = v["video_id"]
        url = f"https://www.youtube.com/watch?v={vid}"
        thumb_url = v["thumbnail_url"]

        log.info(f"[{i}/{len(videos)}] {v['title']} — {url}")

        gemini_reply = ""
        hit = "no"

        if do_hits and thumb_url:
            try:
                img = fetch_bytes(thumb_url)
                mime = guess_mime(thumb_url)
                raw = call_gemini_on_image(img, mime, args.gemini_model, args.gemini_api_key)
                gemini_reply = raw
                yn = normalize_yes_no(raw)
                hit = "yes" if yn == "yes" else "no"
                if hit == "yes":
                    yes_hits += 1
                log.info(f"  -> Gemini: {raw!r} => hit={hit}")
            except Exception as e:
                log.info(f"  ! Error during Gemini call: {e}")
        else:
            log.info("  -> Skipped thumbnail analysis.")

        results.append({
            "video_id": vid,
            "video_title": v["title"],
            "channel_title": v["channel_title"],
            "published_at": v["published_at"],
            "video_url": url,
            "thumbnail_url": thumb_url,
            "gemini_reply": gemini_reply,
            "hit": hit,
        })

        if do_hits:
            time.sleep(args.sleep)

    # Write CSV
    fieldnames = [
        "video_id", "video_title", "channel_title", "published_at",
        "video_url", "thumbnail_url", "gemini_reply", "hit"
    ]
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)

    log.info(f"\nDone. {yes_hits} hit(s) saved to {args.csv} (thumbnail analysis: {'ON' if do_hits else 'OFF'})")

if __name__ == "__main__":
    main()
