FROM python:3.10-slim-bookworm



WORKDIR /app

# Install Java (required by PySpark) and minimal tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark (includes Spark runtime for local jobs)
RUN pip install --no-cache-dir pyspark

# Copy batch job scripts into image
COPY cleaner.py /app/cleaner.py
COPY batch_hourly_aggregates.py /app/batch_hourly_aggregates.py
COPY batch_daily_aggregates.py /app/batch_daily_aggregates.py
COPY dq_report.py /app/dq_report.py

# Create a non-root user
RUN useradd -m appuser && chown -R appuser /app
USER appuser

ENV PYTHONUNBUFFERED=1

# Default to an interactive shell so you can run `spark-submit` or `python` commands
CMD ["bash"]
