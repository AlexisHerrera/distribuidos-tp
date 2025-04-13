FROM python:3.12-slim

WORKDIR /app

COPY src/server/cleaner .
COPY src/common/ common/

ENTRYPOINT ["python", "main.py"]
