# shared/db.py
import os
from sqlalchemy import (create_engine, Column, String, Text, Integer,
                        DateTime, func)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/recap")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True)  # uuid string
    user_id = Column(String, nullable=True)
    package_id = Column(String, nullable=True)
    priority = Column(Integer, default=1)
    file_name = Column(String, nullable=False)
    b2_path = Column(String, nullable=False)
    file_url = Column(Text, nullable=True)
    status = Column(String, default="queued")  # queued, processing, completed, failed
    subject = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)
    full_text = Column(Text, nullable=True)
    result_url = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

class JobUpdate(Base):
    __tablename__ = "job_updates"
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False)
    seq = Column(Integer, nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

def init_db():
    Base.metadata.create_all(bind=engine)
