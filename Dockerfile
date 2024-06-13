FROM apache/airflow:2.9.2
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential libopenmpi-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
# ENV PYTHONPATH="${python_path}:/opt/airflow/dags"
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" mpi4py
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install psycopg2-binary
COPY data_sample /tmp/data_sample
COPY dags /opt/airflow/dags