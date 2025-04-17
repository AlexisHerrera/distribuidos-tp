FROM python:3.12-slim

WORKDIR /app

COPY src/server/cleaner .
COPY src/common/ common/
COPY src/model/ model/
COPY src/utils/ utils/

ENTRYPOINT ["python", "main.py"]
