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
RUN pip install requirements.txt 
    && pip install  torch --index-url https://download.pytorch.org/whl/cpu

COPY . .

CMD ["python", "worker.py"]
