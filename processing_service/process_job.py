# processing_service/process_job.py
import os
import tempfile
import time
import traceback
from shared.db import SessionLocal, Job, JobUpdate
from shared.b2_utils import download_from_b2_to, upload_to_b2, get_signed_url
from dotenv import load_dotenv
load_dotenv()
import requests

# Optional summarization with Groq (if you used it earlier)
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
def groq_generate(prompt, max_tokens=500):
    if not GROQ_API_KEY:
        return None
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {"model":"llama3-70b-8192","messages":[{"role":"user","content":prompt}],"max_tokens":max_tokens}
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

# PhoWhisper via faster-whisper
from faster_whisper import WhisperModel
import torch

PHO_MODEL = os.getenv("PHO_MODEL", "vinai/PhoWhisper-small")
PHO_DEVICE = os.getenv("PHO_DEVICE", "cpu")  # 'cuda' or 'cpu'
_compute_type = "float16" if PHO_DEVICE == "cuda" else "int8_float16"

print("Loading PhoWhisper model:", PHO_MODEL, "device:", PHO_DEVICE)
model = WhisperModel(PHO_MODEL, device=PHO_DEVICE, compute_type=_compute_type)

def transcribe_and_stream(b2_path, job_id, language="vi"):
    """
    Download file, transcribe with faster-whisper, insert JobUpdate records per segment.
    Returns full_text.
    """
    db = SessionLocal()
    tmpfd, tmpfile = tempfile.mkstemp(suffix=os.path.splitext(b2_path)[1])
    os.close(tmpfd)
    try:
        download_from_b2_to(tmpfile, b2_path)
        segments, info = model.transcribe(tmpfile, beam_size=5, language=language if language != 'auto' else None)
        full_parts = []
        seq = 0
        for seg in segments:
            seq += 1
            text = seg.text.strip()
            if not text:
                continue
            # Save partial update
            upd = JobUpdate(job_id=job_id, seq=seq, text=text)
            db.add(upd)
            db.commit()
            full_parts.append(text)
        full_text = " ".join(full_parts)
        return full_text
    finally:
        try:
            os.remove(tmpfile)
        except:
            pass
        db.close()

def process_job(job):
    db = SessionLocal()
    try:
        # 1) mark processing (should already be)
        job.status = "processing"
        db.add(job)
        db.commit()

        # 2) run transcription
        language_guess = "vi"  # you can map package/lang as needed; use request param in job if stored
        full_text = transcribe_and_stream(job.b2_path, job.id, language=language_guess)

        # 3) summarization (optional)
        subject = None
        summary = None
        try:
            subject = groq_generate(f"Hãy cho biết chủ đề chính của nội dung sau bằng tiếng Việt: {full_text[:4000]}", max_tokens=120)
            summary = groq_generate(f"Tóm tắt nội dung sau một cách ngắn gọn, đủ ý trong 500 từ bằng tiếng Việt:\n\n{full_text[:30000]}", max_tokens=600)
        except Exception as e:
            print("Groq summarization failed", e)
            # continue even if summarization fails

        # 4) upload result json to B2
        result_data = {
            "subject": subject,
            "summary": summary,
            "full_text": full_text,
            "file_b2_path": job.b2_path
        }
        import json, tempfile
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tf.write(json.dumps(result_data, ensure_ascii=False).encode("utf-8"))
        tf.flush()
        tf.close()
        json_b2_name = f"results/{job.id}.json"
        upload_to_b2(tf.name, json_b2_name, content_type="application/json")
        try:
            os.remove(tf.name)
        except:
            pass
        result_url = get_signed_url(json_b2_name)

        # 5) update job record
        job.status = "completed"
        job.subject = subject
        job.summary = summary
        job.full_text = full_text
        job.result_url = result_url
        db.add(job)
        db.commit()
        print(f"Job {job.id} completed.")
    except Exception as e:
        db.rollback()
        job.status = "failed"
        job.subject = None
        job.summary = None
        job.full_text = None
        db.add(job)
        db.commit()
        print(f"Job {job.id} failed:", str(e))
        traceback.print_exc()
    finally:
        db.close()
