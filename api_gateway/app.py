# api_gateway/app.py
import os, uuid, time
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from werkzeug.utils import secure_filename
from db import SessionLocal, init_db, Job, JobUpdate
from b2_utils import upload_to_b2, get_signed_url
from sqlalchemy import select
from dotenv import load_dotenv
import json

load_dotenv()
init_db()

ALLOWED_EXT = {'.wav', '.mp3', '.m4a', '.flac', '.pdf', '.docx', '.txt'}

PACKAGE_PRIORITY = {"business": 4, "premium": 3, "plus": 2, "free": 1}

app = Flask(__name__)
CORS(app)

UPLOAD_TMP = os.getenv("TMP_UPLOAD_DIR", "/tmp/recap_uploads")
os.makedirs(UPLOAD_TMP, exist_ok=True)

@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200

@app.route("/submit", methods=["POST"])
def submit():
    if 'file' not in request.files:
        return jsonify({"error": "Missing file"}), 400
    f = request.files['file']
    filename = secure_filename(f.filename or f"upload_{int(time.time())}")
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXT:
        return jsonify({"error": f"Invalid file ext: {ext}"}), 400

    user_id = request.form.get("user_id")  # từ Google login token decode
    package_id = (request.form.get("package_id") or "free").lower()
    language_code = request.form.get("language_code", "auto")

    job_id = str(uuid.uuid4())
    local_path = os.path.join(UPLOAD_TMP, f"{job_id}_{filename}")
    f.save(local_path)

    # Upload file lên B2
    b2_path = f"uploads/{job_id}/{filename}"
    upload_to_b2(local_path, b2_path, content_type="application/octet-stream")
    file_url = get_signed_url(b2_path, valid_seconds=3600)

    priority = PACKAGE_PRIORITY.get(package_id, 1)

    # Insert job DB
    db = SessionLocal()
    job = Job(
        id=job_id, user_id=user_id, package_id=package_id,
        priority=priority, file_name=filename, b2_path=b2_path,
        file_url=file_url, status="queued"
    )
    db.add(job)
    db.commit()
    db.close()

    try: os.remove(local_path)
    except: pass

    return jsonify({"job_id": job_id, "status": "queued"})

@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    db.close()
    if not job: return jsonify({"error": "Not found"}), 404
    return jsonify({"job_id": job.id, "status": job.status})

@app.route("/result/<job_id>", methods=["GET"])
def result(job_id):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    db.close()
    if not job: return jsonify({"error": "job not found"}), 404
    return jsonify({
        "job_id": job.id, "status": job.status,
        "subject": job.subject, "summary": job.summary,
        "full_text": job.full_text, "file_url": job.file_url,
        "result_url": job.result_url
    })

def stream_job_events(job_id):
    db = SessionLocal()
    last_seq = 0
    while True:
        updates = db.query(JobUpdate).filter(
            JobUpdate.job_id == job_id, JobUpdate.seq > last_seq
        ).order_by(JobUpdate.seq).all()
        if updates:
            for u in updates:
                data = {"seq": u.seq, "text": u.text}
                yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                last_seq = u.seq
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job: break
        if job.status in ("completed", "failed"):
            yield f"event: finished\ndata: {json.dumps({'status': job.status})}\n\n"
            break
        time.sleep(1)
    db.close()

@app.route("/stream/<job_id>", methods=["GET"])
def stream(job_id):
    return Response(stream_job_events(job_id), mimetype="text/event-stream")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
