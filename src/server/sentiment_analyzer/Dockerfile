FROM nvidia/cuda:12.4.0-runtime-ubuntu22.04

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3-pip \
      python-is-python3  \
      libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY src/server/sentiment_analyzer/requirements.txt .

RUN pip3 install --upgrade pip && \
    pip3 install torch torchvision torchaudio \
      --index-url https://download.pytorch.org/whl/cu117 && \
    pip3 install --no-cache-dir -r requirements.txt

COPY src/ ./src/

CMD ["python3", "src/server/sentiment_analyzer/main.py"]
