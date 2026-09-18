# Môi trường Data Lakehouse cho TLCN

Môi trường Docker này hiện thực kiến trúc trong tài liệu: nguồn dữ liệu được Airflow điều phối, Spark/PySpark xử lý theo Bronze–Silver–Gold, dữ liệu Parquet/Iceberg nằm trên MinIO, dbt mô hình hóa Gold, Trino phục vụ truy vấn và Power BI kết nối từ máy Windows. Nhánh Python/ML có một Jupyter riêng.

## Thành phần và cổng truy cập

| Thành phần | Địa chỉ từ máy host | Vai trò |
|---|---|---|
| Airflow | http://localhost:8083 | Điều phối pipeline |
| MinIO API | http://localhost:9000 | S3-compatible object storage |
| MinIO Console | http://localhost:9001 | Quản lý bucket |
| Spark/Jupyter | http://localhost:8888 | PySpark notebook |
| Spark UI | http://localhost:8081 | Theo dõi Spark |
| Iceberg REST | http://localhost:8181 | Catalog dùng chung |
| Trino | http://localhost:8082 | SQL query engine |
| ML/Jupyter (tùy chọn) | http://localhost:8889 | Forecasting/ML |

Tài khoản phát triển mặc định:

- Airflow: `airflow` / `airflow`
- MinIO: `admin` / `minioadmin123`
- Trino: không bật xác thực trong môi trường local

## Yêu cầu máy

- Docker Desktop chạy Linux containers với Docker Compose v2.
- Tối thiểu 8 GB RAM dành cho Docker để chạy bộ dữ liệu mẫu; khuyến nghị 12 GB, hoặc 16 GB nếu chạy thêm notebook ML và dữ liệu lớn.
- Khoảng 12–15 GB dung lượng trống cho các image và volume.

## Khởi động

Trong PowerShell tại thư mục dự án:

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose pull
docker compose up -d
docker compose ps
```

Lần đầu cần tải nhiều image nên có thể mất vài phút. `minio-init` và `airflow-init` ở trạng thái `Exited (0)` là đúng: đây là hai job khởi tạo một lần.

### Nếu `docker pull` báo CloudFront `EOF`

Một số mạng có IPv6 nhưng đường IPv6 tới CDN của Docker Hub không ổn định. Trong Docker Desktop, vào **Settings → Resources → Network**, tạm chọn **IPv4 only**, Apply/Restart rồi chạy lại:

```powershell
docker compose pull
```

Sau khi tải xong có thể trả Docker Desktop về **Dual IPv4/IPv6**. Việc chạy lại không tải từ đầu; Docker tiếp tục sử dụng các layer đã cache.

Theo dõi trạng thái nếu một dịch vụ chưa sẵn sàng:

```powershell
docker compose logs -f minio-init iceberg-rest trino
docker compose logs -f airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor
```

## Chạy thử Bronze → Silver → Gold

Job mẫu tạo ba bảng Iceberg trên MinIO:

```powershell
docker compose exec spark-iceberg spark-submit /home/iceberg/jobs/lakehouse_demo.py
```

Kiểm tra bằng Trino CLI trong container:

```powershell
docker compose exec trino trino --execute "SHOW SCHEMAS FROM iceberg"
docker compose exec trino trino --execute "SELECT * FROM iceberg.gold.commodity_year_summary ORDER BY year, commodity"
```

Trong Airflow, bật và chạy thủ công DAG `lakehouse_healthcheck` để xác nhận Airflow truy cập được MinIO, Iceberg REST và Trino qua Docker network.

## Chạy dbt

Sau khi bảng Silver đã được job Spark tạo:

```powershell
docker compose run --rm dbt debug --profiles-dir /usr/app
docker compose run --rm dbt run --profiles-dir /usr/app
docker compose run --rm dbt test --profiles-dir /usr/app
```

dbt đọc `iceberg.silver.agri_trade` và tạo lại bảng tổng hợp trong schema `iceberg.gold`.

## Chạy notebook dự báo

```powershell
docker compose --profile ml up -d ml-notebook
```

Mở http://localhost:8889 hoặc chạy ví dụ trực tiếp:

```powershell
docker compose exec ml-notebook python forecast_demo.py
```

Ví dụ dùng hồi quy tuyến tính để minh họa luồng đọc Gold qua Trino. Đây chưa phải mô hình dự báo dùng cho kết quả nghiên cứu.

## Kết nối Power BI

Power BI Desktop chạy trên Windows, không nằm trong Docker Compose. Dùng driver/connector Trino tương thích và cấu hình:

- Server: `localhost`
- Port: `8082`
- Catalog: `iceberg`
- Schema: `gold`
- User: `powerbi`
- SSL/TLS: tắt cho môi trường local

Không dùng cấu hình không xác thực này khi triển khai thật. Production cần TLS, xác thực Trino, secret manager, phân quyền bucket và tách worker/coordinator phù hợp tải.

## Dừng và làm sạch

```powershell
docker compose down
```

Lệnh trên giữ dữ liệu trong volumes. Chỉ dùng `docker compose down -v` khi chắc chắn muốn xóa toàn bộ dữ liệu MinIO, metadata Airflow và dữ liệu Trino local.
