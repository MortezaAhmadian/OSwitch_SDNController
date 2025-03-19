FROM python:3.9
WORKDIR /app
COPY . /app
ENV PYTHONPATH=/app
RUN pip install kafka-python requests pandas
CMD ["python", "-m", "producer.py"]
