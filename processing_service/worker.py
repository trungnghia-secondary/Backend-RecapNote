# processing_service/worker.py
import time
from db import SessionLocal, Job
from process_job import process_job

def main():
    while True:
        db = SessionLocal()
        job = db.query(Job).filter(Job.status=="queued").order_by(Job.priority.desc()).first()
        if job:
            job.status = "processing"; db.add(job); db.commit()
            process_job(job)
        db.close()
        time.sleep(2)

if __name__ == "__main__":
    main()
