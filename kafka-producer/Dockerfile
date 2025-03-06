FROM python:3.9
WORKDIR /app
COPY OFS.py producer.py .
COPY config .
RUN pip install kafka-python requests
CMD ["python", "producer.py"]
