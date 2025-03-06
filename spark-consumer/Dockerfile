FROM bitnami/spark:latest
WORKDIR /app
COPY consumer.py .
RUN pip install kafka-python pyspark psycopg2-binary
CMD ["python", "consumer.py"]
