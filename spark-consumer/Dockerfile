FROM bitnami/spark:latest
WORKDIR /app
COPY . /app
ENV PYTHONPATH=/app
RUN pip install kafka-python pyspark psycopg2-binary
CMD ["python", "consumer.py"]
