import os
import tempfile
import re
import subprocess
import zipfile
from flask import Flask, render_template, request, send_file, abort

app = Flask(__name__)
scan_progress = {"percent": 0}

# Safety: keep your API key in the environment
# export YOUTUBE_API_KEY="your-key"  (Linux/Mac)
# setx YOUTUBE_API_KEY "your-key"    (Windows PowerShell)
if not os.getenv("YOUTUBE_API_KEY"):
    print("WARNING: YOUTUBE_API_KEY env var is not set. The script will fail without it.")

@app.get("/")
def home():
    return render_template("index.html")


@app.get("/progress")
def progress():
    return scan_progress


@app.post("/scan")
def scan():
    keyword = (request.form.get("keyword") or "").strip()
    max_results = request.form.get("max_results", "50").strip()
    max_comment_pages = request.form.get("max_comment_pages", "10").strip()
    keywords = (request.form.get("keywords") or "whatsapp, contact, call me, price, for sale, ivory, horn").strip()

    if not keyword:
        abort(400, "keyword is required")

   # Create temp file paths for the script to write
    fd_csv, csv_path = tempfile.mkstemp(prefix="yt_", suffix=".csv")
    os.close(fd_csv)
    fd_log, log_path = tempfile.mkstemp(prefix="yt_", suffix=".log")
    os.close(fd_log)

    cmd = [
        "python", "scan_comments.py",
        keyword,
        "--max_results", max_results,
        "--max_comment_pages", max_comment_pages,
        "--csv", csv_path,
        "--log", log_path,
        "--keywords", keywords,
    ]

    progress_re = re.compile(r"\[(\d+)/(\d+)\]")
    scan_progress["percent"] = 0

    try:
         proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for line in proc.stdout:
            m = progress_re.search(line)
            if m:
                idx, total = int(m.group(1)), int(m.group(2))
                if total:
                    scan_progress["percent"] = int(idx / total * 100)

        proc.wait(timeout=1200)
        if proc.returncode != 0:
            err = "Scan failed"
            try:
                 err = proc.stdout.read()[-1500:]
            except Exception:
                pass
            if os.path.exists(csv_path) and os.path.getsize(csv_path) == 0:
                os.remove(csv_path)
            abort(500, f"Scanner error:\n{err}")
    except subprocess.TimeoutExpired:
        proc.kill()
        abort(504, "Scan timed out. Try fewer results or fewer comment pages.")
    finally:
        scan_progress["percent"] = 100

    # Package CSV and log into a zip for download
    fd_zip, zip_path = tempfile.mkstemp(prefix="yt_", suffix=".zip")
    os.close(fd_zip)
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(csv_path, arcname="yt_scan_results.csv")
        zf.write(log_path, arcname="scan_log.txt")
        
    try:
        return send_file(
            zip_path,
            mimetype="application/zip",
            as_attachment=True,
           download_name="yt_scan_results.zip"
        )
    finally:
        # Temp file cleanup after response is sent
        for p in (csv_path, log_path, zip_path):
            try:
                os.remove(p)
            except Exception:
                pass

if __name__ == "__main__":
    # Simple dev server
    app.run(host="127.0.0.1", port=5000, debug=True)
