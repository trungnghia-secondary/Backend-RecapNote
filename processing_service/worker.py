# processing_service/worker.py
import sys
import os

# Lấy đường dẫn tuyệt đối của thư mục cha (ví dụ: 'processing_service')
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

# Thêm thư mục 'shared' vào sys.path
shared_path = os.path.join(parent_dir, '/app/shared')
if shared_path not in sys.path:
    sys.path.insert(0, shared_path)
    
import time
from shared.db import SessionLocal, init_db, Job
from process_job import process_job
from dotenv import load_dotenv
load_dotenv()
init_db()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "3"))  # seconds

# priority order handled in SQL: priority desc, created_at asc
def pick_next_job():
    db = SessionLocal()
    try:
        # Simple pick; in production use SELECT ... FOR UPDATE SKIP LOCKED
        job = db.query(Job).filter(Job.status == "queued").order_by(Job.priority.desc(), Job.created_at.asc()).first()
        if job:
            # claim it
            job.status = "processing"
            db.add(job)
            db.commit()
            # refresh
            db.refresh(job)
            return job
        return None
    finally:
        db.close()

def main_loop():
    print("Worker started, polling for jobs...")
    while True:
        job = pick_next_job()
        if job:
            print("Picked job:", job.id, "priority:", job.priority)
            process_job(job)
        else:
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main_loop()
