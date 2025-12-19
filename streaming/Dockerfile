FROM apache/spark:3.5.3

WORKDIR /app
COPY streaming.py /app/streaming.py

USER root

RUN mkdir -p /checkpoints/aqi_streaming \
    && mkdir -p /home/spark/.ivy2 \
    && chown -R spark:spark /checkpoints /home/spark/.ivy2

USER spark

CMD ["spark-submit", "--master", "spark://spark-master:7077", "/app/streaming.py"]
