FROM python:3.11-slim

RUN pip install --no-cache-dir kafka-python==2.0.2 pymongo==4.8.0

WORKDIR /app
COPY audit_consumer.py /app/audit_consumer.py

CMD ["python", "-u", "audit_consumer.py"]
