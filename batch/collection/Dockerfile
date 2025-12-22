FROM python:3.10-slim-bookworm



WORKDIR /app

# Install minimal build tools (if native wheels are needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
	gcc \
	ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Install Python dependencies first to take advantage of Docker layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY producer.py /app/producer.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "producer.py"]
