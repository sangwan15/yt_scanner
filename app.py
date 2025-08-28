import os
import tempfile
import subprocess
from flask import Flask, render_template, request, send_file, abort

app = Flask(__name__)

# Safety: keep your API key in the environment
# export YOUTUBE_API_KEY="your-key"  (Linux/Mac)
# setx YOUTUBE_API_KEY "your-key"    (Windows PowerShell)
if not os.getenv("YOUTUBE_API_KEY"):
    print("WARNING: YOUTUBE_API_KEY env var is not set. The script will fail without it.")

@app.get("/")
def home():
    return render_template("index.html")

@app.post("/scan")
def scan():
    keyword = (request.form.get("keyword") or "").strip()
    max_results = request.form.get("max_results", "50").strip()
    max_comment_pages = request.form.get("max_comment_pages", "10").strip()
    keywords = (request.form.get("keywords") or "whatsapp, contact, call me, price, for sale, ivory, horn").strip()

    if not keyword:
        abort(400, "keyword is required")

    # Create a temp CSV file path for the script to write
    fd, csv_path = tempfile.mkstemp(prefix="yt_", suffix=".csv")
    os.close(fd)

    cmd = [
        "python", "scan_comments.py",
        keyword,
        "--max_results", max_results,
        "--max_comment_pages", max_comment_pages,
        "--csv", csv_path,
        "--keywords", keywords,
    ]

    try:
        # Run synchronously for a simple demo; keep limits low for speed/quota.
        cp = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
        if cp.returncode != 0:
            # surface a small slice of stderr for debugging
            err = (cp.stderr or "Unknown error")[:1500]
            # Clean up temp file if created but empty
            try:
                if os.path.exists(csv_path) and os.path.getsize(csv_path) == 0:
                    os.remove(csv_path)
            except Exception:
                pass
            abort(500, f"Scanner error:\n{err}")
    except subprocess.TimeoutExpired:
        abort(504, "Scan timed out. Try fewer results or fewer comment pages.")

    # Return the CSV as a download
    try:
        return send_file(
            csv_path,
            mimetype="text/csv",
            as_attachment=True,
            download_name="yt_scan_results.csv"
        )
    finally:
        # Temp file cleanup after response is sent
        try:
            os.remove(csv_path)
        except Exception:
            pass

if __name__ == "__main__":
    # Simple dev server
    app.run(host="127.0.0.1", port=5000, debug=True)
