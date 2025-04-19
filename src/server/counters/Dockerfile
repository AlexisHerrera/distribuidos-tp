FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy all code from src
COPY src/ ./src/

ENTRYPOINT ["python", "src/server/counters/main.py"]
