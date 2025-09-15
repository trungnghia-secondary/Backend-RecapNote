# processing_service/process_job.py
import os, tempfile, traceback, json
from db import SessionLocal, Job, JobUpdate
from b2_utils import download_from_b2_to, upload_to_b2, get_signed_url
from faster_whisper import WhisperModel
from huggingface_hub import hf_hub_download

PHO_MODEL = os.getenv("PHO_MODEL", "vinai/PhoWhisper-small")
PHO_DEVICE = os.getenv("PHO_DEVICE", "cpu")
compute_type = "float16" if PHO_DEVICE == "cuda" else "int8_float16"

hf_hub_download(repo_id=PHO_MODEL, filename="model.bin")
model = WhisperModel(PHO_MODEL, device=PHO_DEVICE, compute_type=compute_type)

def transcribe_and_stream(b2_path, job_id, language="vi"):
    db = SessionLocal()
    tmpfd, tmpfile = tempfile.mkstemp(suffix=os.path.splitext(b2_path)[1])
    os.close(tmpfd)
    try:
        download_from_b2_to(tmpfile, b2_path)
        segments, info = model.transcribe(tmpfile, beam_size=5, language=language)
        full_parts, seq = [], 0
        for seg in segments:
            text = seg.text.strip()
            if not text: continue
            seq += 1
            upd = JobUpdate(job_id=job_id, seq=seq, text=text)
            db.add(upd); db.commit()
            full_parts.append(text)
        return " ".join(full_parts)
    finally:
        os.remove(tmpfile)
        db.close()

def process_job(job):
    db = SessionLocal()
    try:
        job.status = "processing"; db.add(job); db.commit()
        full_text = transcribe_and_stream(job.b2_path, job.id)
        # (Optional) summarization logic tại đây
        result_data = {"full_text": full_text, "file_b2_path": job.b2_path}
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tf.write(json.dumps(result_data, ensure_ascii=False).encode()); tf.flush(); tf.close()
        json_b2_name = f"results/{job.id}.json"
        upload_to_b2(tf.name, json_b2_name, content_type="application/json")
        os.remove(tf.name)
        job.status = "completed"; job.full_text = full_text
        job.result_url = get_signed_url(json_b2_name)
        db.add(job); db.commit()
    except Exception as e:
        db.rollback(); job.status="failed"; db.add(job); db.commit()
        print("Job failed:", str(e)); traceback.print_exc()
    finally:
        db.close()
