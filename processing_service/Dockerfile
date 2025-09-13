# processing_service/Dockerfile
FROM python:3.10-slim

# Cài đặt dependencies hệ thống
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev ffmpeg git curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements và cài đặt
COPY requirements.txt .
# Torch bản CPU (Render không có GPU). Nếu GPU thì thay = torch + cuda
RUN pip install -r requirements.txt 
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu

COPY worker.py /app
COPY process.py /app
COPY b2_utils.py /app
COPY db.py /app

CMD ["python", "process_job", "worker.py"]
