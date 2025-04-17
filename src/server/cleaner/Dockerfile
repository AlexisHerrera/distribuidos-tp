FROM python:3.12-slim

WORKDIR /app

COPY src/server/cleaner .
COPY src/common/ common/
COPY src/model/ model/

ENTRYPOINT ["python", "main.py"]
