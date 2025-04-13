FROM python:3.12-alpine

RUN apk update && apk upgrade

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY src src
COPY tests tests

ENTRYPOINT [ "python3" ]