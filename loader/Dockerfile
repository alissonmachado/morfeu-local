FROM python:3.11-slim

RUN pip install --no-cache-dir "pyiceberg[pyarrow]==0.9.1" pandas==2.2.3

WORKDIR /app
COPY carga_lote.py /app/carga_lote.py
COPY schemas /app/schemas

CMD ["python", "-u", "carga_lote.py"]
