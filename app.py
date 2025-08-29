import os
import tempfile
import re
import subprocess
import zipfile
from flask import Flask, render_template, request, send_file, abort

app = Flask(__name__)
scan_logs: list[str] = []

# Safety: keep your API key in the environment
# export YOUTUBE_API_KEY="your-key"  (Linux/Mac)
# setx YOUTUBE_API_KEY "your-key"    (Windows PowerShell)
if not os.getenv("YOUTUBE_API_KEY"):
    print("WARNING: YOUTUBE_API_KEY env var is not set. The script will fail without it.")


@app.get("/")
def home():
    return render_template("index.html")


@app.get("/logs")
def logs():
    return {"lines": scan_logs}


@app.post("/scan")
def scan():
    keyword = (request.form.get("keyword") or "").strip()
    max_results = request.form.get("max_results", "50").strip()
    language = (request.form.get("language") or "en").strip()
    max_comments = request.form.get("max_comments", "200").strip()

    if not keyword:
        abort(400, "keyword is required")

    # Create temp file paths for the script to write
    fd_csv, csv_path = tempfile.mkstemp(prefix="yt_videos_", suffix=".csv")
    os.close(fd_csv)
    fd_comment, comment_csv_path = tempfile.mkstemp(prefix="yt_comments_", suffix=".csv")
    os.close(fd_comment)

    cmd = [
        "python", "-u", "scan_comments.py",
        keyword,
        "--max_results",
        max_results,
        "--max_comments",
        max_comments,
        "--csv",
        csv_path,
        "--comment_csv",
        comment_csv_path,
        "--language",
        language,
    ]

    scan_logs.clear()

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in proc.stdout:
            scan_logs.append(line)


        proc.wait(timeout=1200)
        if proc.returncode != 0:
            # Tail the last ~50 lines we captured for the error message
            err = "".join(scan_logs[-50:]) or "Scan failed"
            for p in (csv_path, comment_csv_path):
                try:
                    if os.path.exists(p) and os.path.getsize(p) == 0:
                        os.remove(p)
                except Exception:
                    pass
            abort(500, f"Scanner error:\n{err}")

    except subprocess.TimeoutExpired:
        proc.kill()
        abort(504, "Scan timed out. Try fewer results or fewer comment pages.")

    out_path = comment_csv_path if os.path.exists(comment_csv_path) and os.path.getsize(comment_csv_path) > 0 else csv_path

    try:
        return send_file(
            comment_csv_path,
            mimetype="text/csv",
            as_attachment=True,
            download_name="yt_comment_hits.csv",
        )
    finally:
        # Temp file cleanup after response is sent
        for p in (csv_path, comment_csv_path):
            try:
                os.remove(p)
            except Exception:
                pass


if __name__ == "__main__":
    # Simple dev server
    app.run(host="127.0.0.1", port=5000, debug=True)
