# Docker Data Lakehouse

This project provides a local Data Lakehouse environment for collecting, processing, and analyzing agricultural data. The environment runs with Docker Compose and follows the Bronze, Silver, and Gold data architecture.

The current dataset focuses on coffee, pepper, and rubber. It includes production, prices, international trade, weather, risk events, and World Bank data.

## Main components

| Component | Purpose | Local address |
|---|---|---|
| Airflow | Pipeline orchestration | http://localhost:8083 |
| MinIO | S3-compatible data storage | http://localhost:9001 |
| Spark | Data processing and notebooks | http://localhost:8888 |
| Apache Iceberg | Lakehouse table management | http://localhost:8181 |
| Trino | SQL query engine | http://localhost:8082 |
| PostgreSQL | Airflow metadata database | Internal only |

## Start the environment

```powershell
Copy-Item .env.example .env
docker compose pull
docker compose up -d
docker compose ps
```

The `minio-init` and `airflow-init` containers are expected to finish with exit code `0`. They are initialization jobs, not long-running services.

## Download the source data

The Google Drive file list is stored in `data/drive_manifest.csv`. Run the following command to download the current files and remove local CSV files that are no longer listed in the manifest:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\download_drive_data.ps1 -Prune
```

The downloaded files are stored in `data/raw`. To upload them to the MinIO Bronze bucket, run:

```powershell
docker compose run --rm minio-init
```

The download script tracks Google Drive file IDs. A file is downloaded again when it is replaced on Drive, even if its name remains unchanged.

## Run the sample Lakehouse job

```powershell
docker compose exec spark-iceberg spark-submit /home/iceberg/jobs/lakehouse_demo.py
```

The sample job demonstrates the Bronze, Silver, and Gold flow with Spark and Iceberg. Results can be queried through Trino:

```powershell
docker compose exec trino trino --execute "SHOW SCHEMAS FROM iceberg"
docker compose exec trino trino --execute "SELECT * FROM iceberg.gold.commodity_year_summary"
```

## Stop the environment

```powershell
docker compose down
```

This command keeps the Docker volumes. Avoid using `docker compose down -v` unless all local MinIO and Airflow data should be deleted.
