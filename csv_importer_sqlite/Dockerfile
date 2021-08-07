FROM python:2 AS base

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY csv_importer_sqlite.py .

ENTRYPOINT ["python","./csv_importer_sqlite.py"]