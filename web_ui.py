#!/usr/bin/env python3
"""
Web UI for RediSprint - A2A Sprint Planning System
Real-time interface to run sprint automation with file upload input
"""
import os
import sys
import subprocess
import threading
import queue
from datetime import datetime
from flask import Flask, render_template, Response, jsonify, request, send_from_directory
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

APP_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(APP_DIR, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {"csv", "xlsx", "xls"}

process_running = False
output_queue = queue.Queue()
current_process = None


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"status": "error", "message": "No file provided"}), 400

    file = request.files["file"]
    if not file or file.filename == "":
        return jsonify({"status": "error", "message": "No file selected"}), 400

    if not allowed_file(file.filename):
        return jsonify({
            "status": "error",
            "message": "Only CSV and Excel files (.csv, .xlsx, .xls) are supported"
        }), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)

    return jsonify({"status": "success", "path": filepath, "filename": filename})


@app.route("/template")
def download_template():
    return send_from_directory(
        os.path.join(APP_DIR, "sample_data"),
        "sprint_template.csv",
        as_attachment=True
    )


@app.route("/start", methods=["POST"])
def start_process():
    global process_running, current_process

    if process_running:
        return jsonify({"status": "error", "message": "Process already running"}), 400

    data = request.get_json() or {}
    upload_path = data.get("upload_path", "").strip()

    if not upload_path or not os.path.exists(upload_path):
        return jsonify({
            "status": "error",
            "message": "No file uploaded. Please upload a CSV or Excel file first."
        }), 400

    while not output_queue.empty():
        output_queue.get()

    process_running = True
    filename = os.path.basename(upload_path)

    thread = threading.Thread(target=run_agent_process, args=(upload_path,), daemon=True)
    thread.start()

    return jsonify({"status": "success", "message": "Process started", "filename": filename})


@app.route("/stream")
def stream():
    def generate():
        while True:
            if not output_queue.empty():
                line = output_queue.get()
                yield f"data: {line}\n\n"
            else:
                import time
                time.sleep(0.3)
                if not process_running and output_queue.empty():
                    yield "data: [DONE]\n\n"
                    break

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/status")
def status():
    return jsonify({"running": process_running})


def run_agent_process(upload_path: str):
    global process_running, current_process

    env = os.environ.copy()
    env["UPLOAD_FILE_PATH"] = upload_path
    env.pop("GOOGLE_SHEET_ID", None)
    env["PYTHONIOENCODING"] = "utf-8"

    filename = os.path.basename(upload_path)

    try:
        output_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] Starting A2A Sprint Planning System...")
        output_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] Using uploaded file: {filename}\n")

        current_process = subprocess.Popen(
            [sys.executable, "a2a_agents.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            bufsize=1,
            cwd=APP_DIR,
            env=env,
        )

        for line in iter(current_process.stdout.readline, ""):
            if line:
                output_queue.put(line.rstrip())

        current_process.wait()

        if current_process.returncode == 0:
            output_queue.put(f"\n[{datetime.now().strftime('%H:%M:%S')}] Process completed successfully!")
        else:
            output_queue.put(f"\n[{datetime.now().strftime('%H:%M:%S')}] Process failed with exit code {current_process.returncode}")

    except Exception as e:
        output_queue.put(f"\n[{datetime.now().strftime('%H:%M:%S')}] Error: {str(e)}")
    finally:
        process_running = False
        current_process = None


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print("\n" + "=" * 70)
    print("RediSprint - A2A Sprint Planning System")
    print("=" * 70)
    print(f"\nOpen your browser: http://localhost:{port}")
    print("Press Ctrl+C to stop\n")
    app.run(debug=False, host="0.0.0.0", port=port, threaded=True)
