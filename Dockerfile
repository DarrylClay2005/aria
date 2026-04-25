FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    libvulkan1 \
    libgomp1 \
    mesa-vulkan-drivers \
 && rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["python", "aria.py"]
