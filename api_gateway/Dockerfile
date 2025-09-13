# api_gateway/Dockerfile
FROM python:3.10-slim

# Cài đặt dependencies hệ thống
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev ffmpeg git curl \
    && rm -rf /var/lib/apt/lists/*

# Set thư mục làm việc
WORKDIR /app

# Copy requirements và cài đặt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY ./app

# Expose port
EXPOSE 5000

# Chạy bằng gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]
