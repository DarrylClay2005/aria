FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "bot.py"]
RUN apt-get update && apt-get install -y libvulkan1 && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y libgomp1 mesa-vulkan-drivers && rm -rf /var/lib/apt/lists/*
