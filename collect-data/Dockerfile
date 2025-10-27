FROM python:3.10-slim

WORKDIR /app
COPY producer.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "collector.py"]
