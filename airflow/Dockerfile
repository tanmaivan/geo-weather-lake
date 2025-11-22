FROM apache/airflow:3.1.2-python3.11

USER root
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /home/airflow/.duckdb/extensions
COPY extensions /home/airflow/.duckdb/extensions
RUN python -c "\
import duckdb; \
con = duckdb.connect(); \
con.execute(\"INSTALL '/home/airflow/.duckdb/extensions/delta.duckdb_extension.gz'\"); \
con.execute('SELECT * FROM duckdb_extensions();'); \
con.close()"
