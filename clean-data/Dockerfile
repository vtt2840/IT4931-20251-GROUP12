FROM python:3.9-alpine

RUN apk add --no-cache docker-cli

RUN pip install schedule

WORKDIR /app
COPY scheduler.py .

CMD ["python", "scheduler.py"]
