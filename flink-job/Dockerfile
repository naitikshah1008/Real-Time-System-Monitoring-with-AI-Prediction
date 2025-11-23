FROM flink:1.17.1-scala_2.12-java11

WORKDIR /app

COPY flink_job.py .

COPY jars/ ./jars/

CMD ["flink", "run", "-py", "flink_job.py"]
