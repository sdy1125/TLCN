ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.2-python3.12
FROM ${AIRFLOW_BASE_IMAGE}

COPY requirements.txt /tmp/tlcn-airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/tlcn-airflow-requirements.txt
