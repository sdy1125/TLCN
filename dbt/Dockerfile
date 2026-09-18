FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-trino==1.10.3

WORKDIR /usr/app
ENTRYPOINT ["dbt"]
CMD ["debug", "--profiles-dir", "/usr/app"]

