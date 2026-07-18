FROM python:3.11-slim

RUN pip install --no-cache-dir kafka-python==2.0.2 requests==2.32.3

WORKDIR /app
COPY dadosgov_producer.py /app/dadosgov_producer.py

CMD ["python", "-u", "dadosgov_producer.py"]
