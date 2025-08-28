import os
import re
import csv
import time
import requests
from typing import Dict, List, Tuple

YOUTUBE_API_KEY = "AIzaSyBG9p3EOvsfvl6K7QMyF9PP4okVl2CNbgE"

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
COMMENT_THREADS_URL = "https://www.googleapis.com/youtube/v3/commentThreads"

# -------- Phone number detection (Indian-friendly; supports arbitrary separators) --------
PHONE_REGEX = re.compile(
    r"""
    (?<!\d)                 # no digit before
    (?:\+?91\D*)?           # optional +91 with non-digits
    0?\D*                   # optional leading 0 with separators
    [6-9](?:\D*\d){9}       # 10 digits total, allowing separators
    (?!\d)                  # no digit after
    """,
    re.VERBOSE
)

def extract_phone_numbers(text: str) -> List[str]:
    if not text:
        return []
    matches = PHONE_REGEX.findall(text)
    cleaned = []
    for m in matches:
        digits = "".join(ch for ch in m if ch.isdigit())
        if len(digits) >= 10:
            cleaned.append(digits[-10:])
    seen, out = set(), []
    for c in cleaned:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out

# -------- Keyword/phrase detection --------
def normalize_kw_list(raw_list: List[str]) -> List[str]:
    return [k.strip().lower() for k in raw_list if k.strip()]

def find_keywords(text: str, keywords: List[str]) -> List[str]:
    if not text:
        return []
    low = text.lower()
    seen, hits = set(), []
    for k in keywords:
        if k and k in low and k not in seen:
            seen.add(k)
            hits.append(k)
    return hits

# -------- HTTP helper with basic retry/backoff --------
def _get(url: str, params: Dict, max_retries: int = 5, backoff: float = 1.5) -> Dict:
    attempt = 0
    while True:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        attempt += 1
        if attempt > max_retries:
            raise RuntimeError(f"GET {url} failed after {max_retries} retries: {resp.status_code} {resp.text}")
        time.sleep(backoff ** attempt)

# -------- Search up to n videos --------
def search_videos(keyword: str, n: int = 200) -> List[Dict]:
    results = []
    page_token = None
    while len(results) < n:
        to_fetch = min(50, n - len(results))
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
        data = _get(SEARCH_URL, params)
        items = data.get("items", [])
        results.extend(items)
        page_token = data.get("nextPageToken")
        if not page_token or not items:
            break
    return results

# -------- Fetch comments (skip if disabled) --------
def fetch_all_comments(video_id: str, max_pages: int = 5) -> Tuple[bool, List[Tuple[str, str]]]:
    """Return (comments_enabled, comments_list)."""
    comments: List[Tuple[str, str]] = []
    page_token = None
    pages = 0
    while True:
        params = {
            "key": YOUTUBE_API_KEY,
            "part": "snippet,replies",
            "videoId": video_id,
            "maxResults": 100,
            "textFormat": "plainText",
            "order": "time"
        }
        if page_token:
            params["pageToken"] = page_token
        resp = requests.get(COMMENT_THREADS_URL, params=params, timeout=30)
        if resp.status_code != 200:
            err = resp.json().get("error", {})
            reason = ""
            try:
                reason = err.get("errors", [{}])[0].get("reason", "")
            except Exception:
                pass
            if reason == "commentsDisabled":
                return False, []  # comments disabled
            raise RuntimeError(f"Error fetching comments for {video_id}: {resp.text}")
        data = resp.json()
        for item in data.get("items", []):
            top = item.get("snippet", {}).get("topLevelComment", {})
            top_snip = top.get("snippet", {}) if top else {}
            author = top_snip.get("authorDisplayName", "")
            text = top_snip.get("textDisplay", "") or top_snip.get("textOriginal", "")
            if text:
                comments.append((author, text))
            for rep in item.get("replies", {}).get("comments", []) or []:
                rs = rep.get("snippet", {})
                ra = rs.get("authorDisplayName", "")
                rt = rs.get("textDisplay", "") or rs.get("textOriginal", "")
                if rt:
                    comments.append((ra, rt))
        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token or pages >= max_pages:
            break
    return True, comments

def main():
    import argparse
    parser = argparse.ArgumentParser(description="YouTube search → scan COMMENTS for phone numbers + keywords.")
    parser.add_argument("keyword", help="Keyword to search on YouTube")
    parser.add_argument("--max_results", type=int, default=200, help="Number of videos to fetch (search pages of 50)")
    parser.add_argument("--max_comment_pages", type=int, default=5, help="Comment pages per video (100 threads per page)")
    parser.add_argument("--csv", default="yt_mobile_hits.csv", help="Output CSV filename")
    parser.add_argument(
        "--keywords",
        default="whatsapp, contact, call me, for sale, price, deal, DM, inbox, poach, ivory, skin, horn, leopard, tiger",
        help="Comma-separated keywords/phrases to scan in comments"
    )
    parser.add_argument("--keywords_file", default=None, help="Optional path to a newline-separated keyword list")
    args = parser.parse_args()

    if not YOUTUBE_API_KEY or YOUTUBE_API_KEY == "YOUR_API_KEY_HERE":
        raise SystemExit("Please set YOUTUBE_API_KEY env var or paste your key in the script.")

    # Compose keyword list
    kw_list = normalize_kw_list(args.keywords.split(","))
    if args.keywords_file:
        with open(args.keywords_file, "r", encoding="utf-8") as f:
            kw_list.extend(normalize_kw_list(f.readlines()))
        kw_list = list(dict.fromkeys(kw_list))  # dedupe

    print(f"Searching YouTube for '{args.keyword}' (up to {args.max_results} videos)...")
    search_items = search_videos(args.keyword, n=args.max_results)

    video_meta = []
    for it in search_items:
        vid = it.get("id", {}).get("videoId")
        sn = it.get("snippet", {}) or {}
        if vid:
            video_meta.append({
                "video_id": vid,
                "title": sn.get("title", ""),
                "channel_title": sn.get("channelTitle", "")
            })

    print(f"Found {len(video_meta)} video IDs. Scanning comments only...")

    hits = []
    for idx, vm in enumerate(video_meta, 1):
        vid = vm["video_id"]
        title = vm["title"]
        channel_title = vm["channel_title"]
        url = f"https://www.youtube.com/watch?v={vid}"
        print(f"[{idx}/{len(video_meta)}] {title} — {url}")

        try:
            enabled, comments = fetch_all_comments(vid, max_pages=args.max_comment_pages)
            if not enabled:
                print("  -> Comments disabled. Skipping.")
                continue
        except Exception as e:
            print(f"  ! Error fetching comments: {e}")
            continue

        any_hit = False
        for author, text in comments:
            nums = extract_phone_numbers(text)
            kws = find_keywords(text, kw_list)
            if nums or kws:
                any_hit = True
                hits.append({
                    "where": "comment",
                    "video_id": vid,
                    "video_title": title,
                    "channel_title": channel_title,
                    "video_url": url,
                    "author_or_field": author,
                    "text_snippet": text.replace("\n", " ").strip(),
                    "matched_numbers": ", ".join(nums),
                    "matched_keywords": ", ".join(kws)
                })
        if any_hit:
            print("  -> Matches found in comments.")
        else:
            print("  -> No matches in comments.")

    # Write CSV
    fieldnames = [
        "where", "video_id", "video_title", "channel_title", "video_url",
        "author_or_field", "text_snippet", "matched_numbers", "matched_keywords"
    ]
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hits)

    print(f"\nDone. {len(hits)} hit(s) saved to {args.csv}")

if __name__ == "__main__":
    main()
