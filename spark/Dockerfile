FROM tabulario/spark-iceberg:3.5.5_1.8.1
USER root
COPY requirements.txt /tmp/silver-requirements.txt
RUN pip install --no-cache-dir -r /tmp/silver-requirements.txt
ENV PYTHONPATH=/opt/silver
