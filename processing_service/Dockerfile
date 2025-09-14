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
RUN pip install --no-cache-dir --force-reinstall ctranslate2==3.24.0 \
    --index-url https://pypi.org/simple/ctranslate2/cpu

COPY worker.py /app
COPY process_job.py /app
COPY b2_utils.py /app
COPY db.py /app

EXPOSE 8000

CMD ["python", "0.0.0.0:8000", "process_job.py", "worker.py", "b2_utils.py", "db.py"]
