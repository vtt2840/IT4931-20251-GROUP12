FROM python:3.10-slim

WORKDIR /app
COPY producer.py .
COPY requirements.txt .
COPY .env .  

RUN pip install -r requirements.txt

CMD ["python", "producer.py"]
