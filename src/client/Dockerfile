FROM python:3.12-slim

WORKDIR /app

COPY src/client/ .
COPY src/common/ common/

ENTRYPOINT ["python", "main.py", ".data/movies_metadata.csv", ".data/ratings.csv", ".data/credits.csv"]
