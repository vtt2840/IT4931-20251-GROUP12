FROM apache/spark:3.5.3
WORKDIR /app
COPY streaming.py /app/streaming.py
USER root
RUN mkdir -p /home/spark/.ivy2 && chown -R spark:spark /home/spark/.ivy2
USER spark
CMD ["spark-submit", "--master", "local[*]", "/app/streaming.py"]
