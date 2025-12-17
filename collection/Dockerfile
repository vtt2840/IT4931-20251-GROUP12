FROM python:3.10

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python", "producer.py"]