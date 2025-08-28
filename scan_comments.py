#!/usr/bin/env python3
import os
import csv
import time
import base64
import logging
import requests
import re
from typing import Dict, List, Optional
from urllib.parse import urlparse

# --- Config / Keys (YouTube key always required) ---
YOUTUBE_API_KEY = "AIzaSyBG9p3EOvsfvl6K7QMyF9PP4okVl2CNbgE"
if not YOUTUBE_API_KEY:
    raise SystemExit("Please set YOUTUBE_API_KEY")

# --- Endpoints ---
YT_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YT_COMMENTS_URL = "https://www.googleapis.com/youtube/v3/commentThreads"


# --- Prompt for Gemini ---
PROMPT = (
    "Identify whether the image contains an animal or an illegally traded wilidlife product.Reply strictly just with a yes or no"
)

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("yt_thumbs_gemini")


SCANWORDS: Dict[str, set[str]] = {
    # English
    "en": {
        # wildlife terms
        "pangolin", "ivory", "rhino horn", "tiger skin", "wildlife trade",
        # commerce / contact cues
        "price", "rate", "deal", "for sale", "sell", "selling", "buy", "buying",
        "available", "in stock", "stock",
        "call me", "call", "contact", "phone", "number",
        "dm", "pm", "inbox", "message", "msg",
        "whatsapp", "whats app", "watsapp", "whtsapp", "wa", "wa no", "whatsapp me",
        # common misspellings/short forms
        "whatsap", "watsap", "whatsp",
    },

    # Hindi
    "hi": {
        # wildlife terms (add more if you like)
        "पैंगोलिन", "हाथीदांत", "गैंडे का सींग", "बाघ की खाल", "वन्यजीव व्यापार",
        # commerce / contact cues
        "कीमत", "दाम", "रेट", "डील",
        "कॉल करो", "मुझे कॉल करो", "कॉल करें", "कॉल",
        "संपर्क", "नंबर", "फोन",
        "डीएम", "डायरेक्ट मैसेज", "मैसेज", "संदेश", "इनबॉक्स",
        "व्हाट्सएप", "वॉट्सऐप", "व्हाट्सअप",
        # people often use Latin spellings in Hindi comments too:
        "whatsapp", "watsapp", "wa", "wa no",
    },

    # Marathi
    "mr": {
        # wildlife terms
        "खवले मांजर", "हत्तीचे दात", "गेंड्याचे शिंग", "वाघाची कातडी", "वन्यजीव व्यापार",
        # commerce / contact cues
        "किंमत", "भाव", "रेट", "डील",
        "कॉल करा", "मला कॉल करा", "कॉल", "फोन", "नंबर", "संपर्क",
        "मेसेज", "संदेश", "डीएम", "डायरेक्ट मेसेज", "इनबॉक्स",
        "व्हॉट्सअ‍ॅप", "वॉट्सॅप",
        # common Latin forms used in MR contexts
        "whatsapp", "watsapp", "wa", "wa no",
    },

    # Telugu
    "te": {
        # wildlife terms
        "ప్యాంగోలిన్", "దంతం", "ఖడ్గమృగం కొమ్ము", "పులి చర్మం", "అడవి జంతు వాణిజ్యం",
        # commerce / contact cues
        "ధర", "రేటు", "డీల్",
        "కాల్ చేయి", "నన్ను కాల్ చేయి", "కాల్చేయి", "ఫోన్", "నెంబర్",
        "డీఎం", "మెసేజ్", "సందేశం", "ఇన్‌బాక్స్",
        "వాట్సాప్", "వాట్సాప్ నంబర్",
        # Latin spellings seen in TE comments
        "whatsapp", "watsapp", "wa", "wa no",
    },
}


# Regex to match Indian mobile numbers, allowing special characters between
# digits (e.g. 7@8#6*6&6*9%6(5#8#8).
INDIAN_MOBILE_REGEX = re.compile(
    r"(?:\+?91[\D]*)?([6-9](?:[\D]*\d){9})"
)


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
            "regionCode": "IN",        # bias results to India (availability/ranking)
            "relevanceLanguage": "hi", # or "en" or leave out; biases query language
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

def fetch_comments(video_id: str, max_results: int = 50) -> List[Dict]:
    """Fetch up to max_results top-level comments for a video."""
    items: List[Dict] = []
    page_token: Optional[str] = None
    while len(items) < max_results:
        to_fetch = min(100, max_results - len(items))
        params = {
            "key": YOUTUBE_API_KEY,
            "part": "snippet",
            "videoId": video_id,
            "maxResults": to_fetch,
            "textFormat": "plainText",
        }
        if page_token:
            params["pageToken"] = page_token
        data = _get(YT_COMMENTS_URL, params)
        page_items = data.get("items", [])
        items.extend(page_items)
        page_token = data.get("nextPageToken")
        if not page_token or not page_items:
            break

    comments: List[Dict] = []
    for it in items:
        sn = (((it.get("snippet") or {}).get("topLevelComment") or {}).get("snippet")) or {}
        comments.append({
            "comment_id": it.get("id", ""),
            "author": sn.get("authorDisplayName", ""),
            "text": sn.get("textDisplay", ""),
        })
    return comments


def scan_comment(text: str, lang: str) -> Optional[Dict[str, List[str]]]:
    """Return matched words and phone numbers in a comment."""
    words = SCANWORDS.get(lang, set())
    lower_text = text.lower()
    matched_words = [w for w in words if w.lower() in lower_text]
    numbers = [re.sub(r"\D", "", m) for m in INDIAN_MOBILE_REGEX.findall(text)]
    if matched_words or numbers:
        return {"words": matched_words, "numbers": numbers}
    return None

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
    ap.add_argument(
        "--language",
        choices=["hi", "en", "mr"],
        default="en",
        help="Language for comment scanwords (hi=en for Telugu per requirement)",
    )
    ap.add_argument(
        "--max_comments",
        type=int,
        default=200,
        help="Max comments to fetch per video",
    )
    ap.add_argument(
        "--comment_csv",
        default="yt_comment_hits.csv",
        help="Output CSV filename for comment hits",
    )
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

    results: List[Dict] = []
    comment_hits: List[Dict] = []


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

        # Fetch and scan comments
        try:
            comments = fetch_comments(vid, max_results=args.max_comments)
            for c in comments:
                scan_res = scan_comment(c["text"], args.language)
                if scan_res:
                    comment_hits.append({
                        "video_id": vid,
                        "video_title": v["title"],
                        "comment_id": c["comment_id"],
                        "author": c["author"],
                        "text": c["text"],
                        "matched_words": ",".join(scan_res["words"]),
                        "phone_numbers": ",".join(scan_res["numbers"]),
                    })
        except Exception as e:
            log.info(f"  ! Error fetching comments: {e}")

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

    if comment_hits:
        comment_fields = [
            "video_id",
            "video_title",
            "comment_id",
            "author",
            "text",
            "matched_words",
            "phone_numbers",
        ]
        with open(args.comment_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=comment_fields)
            w.writeheader()
            w.writerows(comment_hits)
        log.info(f"Saved {len(comment_hits)} comment hit(s) to {args.comment_csv}")
    else:
        log.info("No comment hits found.")


    log.info(f"\nDone. {yes_hits} hit(s) saved to {args.csv} (thumbnail analysis: {'ON' if do_hits else 'OFF'})")

if __name__ == "__main__":
    main()
