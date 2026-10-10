# RUNBOOK VẬN HÀNH TẦNG SILVER

Runbook này dùng để khởi động hệ thống và chạy pipeline Silver thật qua Apache
Airflow. Pipeline lấy dữ liệu từ các Bronze object bất biến trên MinIO, xử lý bằng
Spark và ghi vào các bảng Apache Iceberg. Không đọc `data/raw` trong đường chạy
Silver production.

## 1. Thư mục làm việc và địa chỉ dịch vụ

Mở terminal tại thư mục repository:

```bash
cd '/run/media/sduy/New Volume1/TLCN'
```

Các địa chỉ dùng khi vận hành:

| Dịch vụ | Địa chỉ |
| --- | --- |
| Airflow | http://localhost:8083 |
| Trino | http://localhost:8082 |
| MinIO API | http://localhost:9000 |
| MinIO Console | http://localhost:9001 |
| Iceberg REST | http://localhost:8181 |
| Spark UI | http://localhost:8081 |

Airflow 3 dùng service `airflow-apiserver`; repository không có service
`airflow-webserver`.

Dữ liệu Silver và Gold không nằm trong các bucket MinIO riêng. Iceberg REST
catalog dùng warehouse `s3://warehouse/`, vì vậy file Parquet và metadata thật
nằm dưới `warehouse/silver/` và `warehouse/gold/`. Tên `iceberg.silver` hoặc
`iceberg.gold` trong Trino gồm catalog và schema; chúng không ánh xạ thành bucket
`silver` hoặc `gold` riêng.

## 2. Chuẩn bị biến môi trường

Tạo `.env` nếu chưa có:

```bash
cp .env.example .env
```

Kiểm tra các biến bắt buộc trong `.env`:

```dotenv
POSTGRES_PASSWORD=airflow
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=minioadmin123
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASSWORD=airflow
SILVER_RUNNER_TOKEN=replace-with-at-least-32-random-characters
SILVER_DAG_SCHEDULE=
```

`SILVER_RUNNER_TOKEN` phải có ít nhất 32 ký tự và phải giống nhau trong Airflow và
Spark Runner. Compose tự truyền cùng biến này vào cả hai service. Không in token
vào log hoặc đưa ảnh `.env` vào báo cáo. Để `SILVER_DAG_SCHEDULE` trống nếu chỉ
muốn chạy thủ công.

Sau khi đổi `.env`, recreate các service sử dụng biến môi trường:

```bash
docker compose up -d --force-recreate \
  spark-iceberg airflow-apiserver airflow-scheduler \
  airflow-dag-processor airflow-triggerer
```

## 3. Khởi động toàn bộ hạ tầng cần cho Silver

Lần đầu hoặc sau khi thay Dockerfile, build image:

```bash
docker compose build minio airflow-init spark-iceberg
```

Khởi động storage, catalog và database:

```bash
docker compose up -d postgres minio minio-init iceberg-rest trino spark-iceberg
```

Khởi tạo hoặc migrate database Airflow:

```bash
docker compose up -d airflow-init
docker compose wait airflow-init
```

Khởi động đầy đủ Airflow 3:

```bash
docker compose up -d \
  airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

Trong những lần chạy sau, khi image và database đã sẵn sàng, có thể dùng một lệnh:

```bash
docker compose up -d \
  postgres minio minio-init iceberg-rest trino spark-iceberg \
  airflow-init airflow-apiserver airflow-scheduler \
  airflow-dag-processor airflow-triggerer
```

## 4. Kiểm tra dịch vụ trước khi chạy

Xem trạng thái container:

```bash
docker compose ps
```

Các service chính cần ở trạng thái `Up`; PostgreSQL, Trino và
`airflow-apiserver` nên hiển thị `healthy`. `airflow-init` và `minio-init` kết thúc
với exit code 0 là bình thường.

Kiểm tra Airflow API/UI:

```bash
curl --fail http://localhost:8083/api/v2/monitor/health
```

Kiểm tra Trino:

```bash
curl --fail http://localhost:8082/v1/info
```

Kiểm tra Spark Runner từ bên trong mạng Compose:

```bash
docker compose exec airflow-scheduler \
  python -c "import urllib.request; print(urllib.request.urlopen('http://spark-iceberg:8090/health').read().decode())"
```

Kiểm tra DAG đã được Airflow nạp:

```bash
docker compose exec airflow-scheduler airflow dags list | grep silver_transformation
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Nếu `list-import-errors` không trả dòng lỗi thì DAG đã được import thành công.

## 5. Điều kiện Bronze trước khi chạy Silver

Silver chỉ đọc metadata:

```text
s3://bronze/_metadata/latest_success/{dataset_id}.json
```

Nếu 26 Bronze version chưa tồn tại, chạy Bronze ingestion thật trước:

```bash
docker compose exec airflow-scheduler airflow dags unpause bronze_ingestion
docker compose exec airflow-scheduler airflow dags trigger bronze_ingestion
```

Theo dõi Bronze trên Airflow UI và chỉ chạy Silver khi Bronze đã hoàn tất. Không
sửa object Bronze hoặc metadata `latest_success` bằng tay.

## 6. Mở và chuẩn bị DAG Silver

Mở giao diện:

```text
http://localhost:8083
```

Thông tin mặc định là `airflow/airflow`, trừ khi
`AIRFLOW_ADMIN_USER` và `AIRFLOW_ADMIN_PASSWORD` trong `.env` đã được thay đổi.

Bỏ pause DAG bằng CLI:

```bash
docker compose exec airflow-scheduler \
  airflow dags unpause silver_transformation
```

Xem danh sách dataset hợp lệ nếu cần chọn nguồn:

```bash
docker compose exec spark-iceberg \
  python3 -c "from silver.registry import SOURCES; print('\n'.join(SOURCES))"
```

## 7. Chạy Silver thật qua Airflow

### 7.1. Chạy toàn bộ 26 dataset

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger silver_transformation
```

Đây là lệnh chạy production path thật: Airflow gửi request có Bearer token đến
Spark Runner, Runner gọi `spark-submit`, Spark đọc Bronze từ MinIO và ghi Iceberg.
Đây là lần chạy được lưu đầy đủ trong lịch sử DAG của Airflow.

### 7.2. Chạy một dataset

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger silver_transformation \
  --conf '{"dataset_id":"faostat_production_coffee"}'
```

### 7.3. Chạy một danh sách dataset

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger silver_transformation \
  --conf '{"dataset_id":["faostat_production_coffee","world_bank_wdi"]}'
```

### 7.4. Chạy bằng giao diện Airflow

1. Mở `http://localhost:8083`.
2. Chọn DAG `silver_transformation`.
3. Chọn **Trigger DAG**.
4. Để config `{}` nếu chạy toàn bộ.
5. Với một dataset, nhập:

```json
{"dataset_id":"faostat_production_coffee"}
```

6. Với nhiều dataset, nhập:

```json
{"dataset_id":["faostat_production_coffee","world_bank_wdi"]}
```

7. Xác nhận trigger và theo dõi tab **Grid** hoặc **Graph**.

## 8. Theo dõi lần chạy

Liệt kê các DAG run mới nhất:

```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs silver_transformation --output table
```

Sau khi lấy được `run_id`, xem trạng thái các task:

```bash
RUN_ID='manual__YYYY-MM-DDTHH:MM:SS+00:00'
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run silver_transformation "$RUN_ID"
```

Theo dõi log các service:

```bash
docker compose logs --tail=200 -f \
  airflow-scheduler airflow-dag-processor airflow-triggerer spark-iceberg
```

Nhấn `Ctrl+C` chỉ dừng việc xem log, không dừng container hoặc DAG run.

DAG có bốn task:

```text
select_datasets
  → submit_spark_jobs
  → wait_for_completion
  → publish_summary
```

`wait_for_completion` poll Runner mỗi 15 giây. Spark job có giới hạn 2 giờ;
Airflow task có hard timeout 3 giờ.

## 9. Đọc kết quả Spark Runner

Runner lưu artifact trong Docker volume `silver-job-logs`, được mount tại
`/tmp/silver-jobs` trong container `spark-iceberg`.

Liệt kê job gần nhất:

```bash
docker compose exec spark-iceberg \
  sh -lc 'find /tmp/silver-jobs -mindepth 1 -maxdepth 1 -type d -printf "%T@ %f\n" | sort -nr | head'
```

Xem request, exit code và kết quả của một job:

```bash
JOB_ID='<64-ký-tự-job-id>'
docker compose exec spark-iceberg cat "/tmp/silver-jobs/$JOB_ID/request.json"
docker compose exec spark-iceberg cat "/tmp/silver-jobs/$JOB_ID/exit.json"
docker compose exec spark-iceberg cat "/tmp/silver-jobs/$JOB_ID/results.json"
```

Xem cuối Spark log:

```bash
docker compose exec spark-iceberg \
  tail -n 200 "/tmp/silver-jobs/$JOB_ID/spark.log"
```

Kết quả mỗi dataset có thể là:

- `SUCCESS`: publish thành công, không có warning.
- `WARNING`: publish thành công nhưng có missing ratio, metadata hoặc provenance cần chú ý.
- `QUARANTINED`: winner hợp lệ đã publish và có dòng warning/error được lưu quarantine theo rule cho phép.
- `SKIPPED`: cùng Bronze checksum và transform version đã được publish; snapshot hiện hành vẫn hợp lệ.
- `FAILED`: contract, dữ liệu hoặc hạ tầng làm dataset không thể hoàn tất.

`SKIPPED` trong một real run là hành vi idempotent. Không xóa `processing_state` để
ép chạy lại và không sửa checksum/version bằng tay.

## 10. Kiểm tra dữ liệu thật bằng Trino

Liệt kê chín bảng Silver:

```bash
docker compose exec trino trino --execute \
  'SHOW TABLES FROM iceberg.silver'
```

Đếm số dòng sáu bảng miền:

```bash
docker compose exec trino trino --execute "
SELECT 'production_national' AS table_name, COUNT(*) AS rows FROM iceberg.silver.production_national
UNION ALL SELECT 'production_provincial', COUNT(*) FROM iceberg.silver.production_provincial
UNION ALL SELECT 'market_indicator', COUNT(*) FROM iceberg.silver.market_indicator
UNION ALL SELECT 'trade_partner', COUNT(*) FROM iceberg.silver.trade_partner
UNION ALL SELECT 'trade_total', COUNT(*) FROM iceberg.silver.trade_total
UNION ALL SELECT 'weather_daily', COUNT(*) FROM iceberg.silver.weather_daily
ORDER BY table_name"
```

Kiểm tra không có duplicate `source_record_id`:

```bash
docker compose exec trino trino --execute "
SELECT 'production_national' AS table_name, COUNT(*) AS rows,
       COUNT(DISTINCT source_record_id) AS unique_rows
FROM iceberg.silver.production_national
UNION ALL
SELECT 'production_provincial', COUNT(*), COUNT(DISTINCT source_record_id)
FROM iceberg.silver.production_provincial
UNION ALL
SELECT 'market_indicator', COUNT(*), COUNT(DISTINCT source_record_id)
FROM iceberg.silver.market_indicator
UNION ALL
SELECT 'trade_partner', COUNT(*), COUNT(DISTINCT source_record_id)
FROM iceberg.silver.trade_partner
UNION ALL
SELECT 'trade_total', COUNT(*), COUNT(DISTINCT source_record_id)
FROM iceberg.silver.trade_total
UNION ALL
SELECT 'weather_daily', COUNT(*), COUNT(DISTINCT source_record_id)
FROM iceberg.silver.weather_daily"
```

Xem DQ mới nhất:

```bash
docker compose exec trino trino --execute "
SELECT run_id, dataset_id, status, severity, checked_at
FROM iceberg.silver.dq_results
WHERE check_name = 'audit_transform_publish'
ORDER BY checked_at DESC
LIMIT 30"
```

Xem quarantine mới nhất:

```bash
docker compose exec trino trino --execute "
SELECT dataset_id, error_code, error_severity, COUNT(*) AS records
FROM iceberg.silver.quarantine_records
GROUP BY dataset_id, error_code, error_severity
ORDER BY dataset_id, error_code"
```

Xem processing state mới nhất của từng dataset:

```bash
docker compose exec trino trino --execute "
WITH ranked AS (
  SELECT dataset_id, status, target_contract, output_rows, snapshot_id,
         transform_version, checked_at,
         ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY checked_at DESC) AS rn
  FROM iceberg.silver.processing_state
)
SELECT dataset_id, status, target_contract, output_rows, snapshot_id,
       transform_version, checked_at
FROM ranked
WHERE rn = 1
ORDER BY dataset_id"
```

Có thể chạy cùng SQL trong DBeaver với host `localhost`, port `8082`, catalog
`iceberg`, schema `silver`.

## 11. Chạy trực tiếp bằng Spark khi Airflow không khả dụng

Chỉ dùng đường này để vận hành khẩn cấp hoặc chẩn đoán, vì nó bỏ qua lịch sử task
của Airflow nhưng vẫn chạy pipeline và ghi state/DQ thật.

Một dataset:

```bash
RUN_ID="manual-direct-$(date -u +%Y%m%dT%H%M%SZ)"
docker compose exec spark-iceberg \
  /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/silver/jobs/silver_pipeline.py \
  --run-id "$RUN_ID" \
  --dataset-id faostat_production_coffee
```

Nhiều dataset:

```bash
RUN_ID="manual-direct-$(date -u +%Y%m%dT%H%M%SZ)"
docker compose exec spark-iceberg \
  /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/silver/jobs/silver_pipeline.py \
  --run-id "$RUN_ID" \
  --dataset-id faostat_production_coffee \
  --dataset-id world_bank_wdi
```

Toàn bộ 26 dataset: bỏ tất cả flag `--dataset-id`.

## 12. Xử lý lỗi vận hành

### Airflow UI không mở ở cổng 8083

```bash
docker compose ps airflow-apiserver
docker compose logs --tail=200 airflow-apiserver
```

Khởi động lại đúng service Airflow 3:

```bash
docker compose up -d airflow-apiserver
```

### DAG không xuất hiện

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail=200 airflow-dag-processor
```

Sau khi sửa file DAG, restart processor và scheduler:

```bash
docker compose restart airflow-dag-processor airflow-scheduler
```

### Scheduler hoặc DAG processor chưa healthy

```bash
docker compose ps airflow-scheduler airflow-dag-processor
docker compose logs --tail=200 airflow-scheduler airflow-dag-processor
```

### Spark Runner từ chối authentication

Kiểm tra `SILVER_RUNNER_TOKEN` trong `.env` dài ít nhất 32 ký tự rồi recreate cả
Spark Runner và các service Airflow:

```bash
docker compose up -d --force-recreate \
  spark-iceberg airflow-apiserver airflow-scheduler \
  airflow-dag-processor airflow-triggerer
```

### Runner báo busy

Runner chỉ xử lý một job tại một thời điểm. Xem DAG run và log hiện hành, chờ job
kết thúc rồi trigger run mới. Không scale nhiều replica `spark-iceberg` cùng ghi
namespace `silver`.

### Dataset FAILED

1. Mở log task `publish_summary` và Spark log.
2. Xem `dq_results`, `quarantine_records`, `processing_state` bằng các câu SQL ở mục 10.
3. Nếu lỗi contract/mapping, cập nhật code hoặc config có review rồi tạo run mới.
4. Nếu lỗi dữ liệu nguồn, sửa ở upstream, chạy Bronze ingestion để tạo object bất biến mới rồi chạy Silver.
5. Không chỉnh trực tiếp bảng Silver, object Bronze hoặc control tables để che lỗi.

### Process dừng sau khi MERGE

Chạy lại cùng dataset bằng một Airflow run mới. Pipeline transform lại và kiểm tra
checksum, transform version, row count, uniqueness và tập `source_record_id`. Nếu
scope đã khớp snapshot hiện hành, state được phục hồi mà không tạo MERGE mới.

## 13. Restart và dừng hệ thống

Restart riêng Airflow:

```bash
docker compose restart \
  airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

Restart Spark Runner và Trino:

```bash
docker compose restart spark-iceberg trino
```

Dừng container nhưng giữ volumes và dữ liệu:

```bash
docker compose stop
```

Khởi động lại các container đã dừng:

```bash
docker compose start
```

Gỡ container/network nhưng giữ named volumes:

```bash
docker compose down
```

Không dùng `docker compose down -v` trong vận hành bình thường vì lệnh đó xóa
PostgreSQL, MinIO, Trino và job-log volumes.

## 14. Quy tắc dữ liệu cần nhớ khi vận hành

- Missing hợp lệ giữ `NULL` và `observation_status=missing`; số 0 vẫn là giá trị thật.
- NASA `-999` được chuyển thành `NULL`; giá trị ngoài miền bị quarantine.
- Exact duplicate giữ một observation deterministic.
- Conflicting duplicate mặc định chặn publish; ngoại lệ giá cà phê chỉ áp dụng khi đúng rule versioned.
- UN Comtrade giữ dòng tổng khi có tổng tương ứng để tránh double count.
- Không tự đổi tỷ giá VND/LCU/SLC/USD.
- Không tự ép mã lịch sử/aggregate sang ISO3 khi chưa có crosswalk được duyệt.
- `solar_radiation` có unit provenance `UNVERIFIED`; không quy đổi trước khi xác minh.
- Không expire Iceberg snapshot còn được `processing_state` tham chiếu.
- Không chạy đồng thời nhiều Spark Runner ghi cùng namespace Silver.
