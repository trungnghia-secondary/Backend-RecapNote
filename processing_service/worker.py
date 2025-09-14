# processing_service/worker.py
import sys
import os
import time
from db import SessionLocal, init_db, Job
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
