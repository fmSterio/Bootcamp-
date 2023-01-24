FROM python:3.11

RUN pip install pandas sqlalchemy psycopg2-binary requests

WORKDIR /app

COPY ingest_hw_data.py ingest_hw_data.py 

ENTRYPOINT [ "python","ingest_hw_data.py" ]