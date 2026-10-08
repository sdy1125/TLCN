# Silver: vận hành và giới hạn

Pipeline đọc canonical `_metadata/latest_success/{dataset_id}.json` trong Bronze,
kiểm tra trạng thái, identity, kích thước, schema và SHA-256, rồi ghi Iceberg
`lakehouse.silver`. Không đọc `data/raw` trong đường chạy production. Bản sao
Bronze được tải streaming xuống thư mục tạm, checksum xác minh trước khi đọc;
file tạm tự dọn khi kết thúc. NASA được Spark xử lý bằng biểu thức SQL, không
collect toàn bộ về driver. XLSX dùng openpyxl read-only, chuyển CSV tạm, giới hạn
file 20 MiB. Reader dùng schema string tường minh; output dùng StructType riêng.

## Khởi động và chạy

```bash
docker compose build spark-iceberg
docker compose up -d spark-iceberg trino
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

Để chạy qua Airflow (Bronze phải đã có successful versions):

```bash
# Toàn bộ 26 dataset
docker compose exec airflow-scheduler airflow dags trigger silver_transformation
# Một dataset
docker compose exec airflow-scheduler airflow dags trigger silver_transformation \
  --conf '{"dataset_id":"faostat_production_coffee"}'
# Danh sách
docker compose exec airflow-scheduler airflow dags trigger silver_transformation \
  --conf '{"dataset_id":["faostat_production_coffee","world_bank_wdi"]}'
```

`SILVER_DAG_SCHEDULE` để trống là manual. `SILVER_RUNNER_URL` trong Compose trỏ
`http://spark-iceberg:8090`, chỉ mạng nội bộ, không mở port host. Airflow không cần
Spark provider/Java/Docker socket. DAG chọn dataset, POST job, poll tới khi kết
thúc rồi kiểm tra summary. Resolve, input audit, transform, output audit và publish
được thực thi trong Spark process để audit và publication dùng cùng input đã
đóng băng. Không có task giả lập bằng echo. Airflow retries cho lỗi gọi API;
contract failures chấm dứt DAG, không retry phép chọn dữ liệu thắng.

Job chạy `spark-submit --master local[2]`, giới hạn 2 giờ, heap 2 GiB, cache trên
đĩa, batch 1.000 và chia CSV NASA thành các input partitions tối đa 16 MiB.
Mỗi dataset được ghi trạng thái riêng; lỗi của một nguồn không ngăn xử lý nguồn
khác. CLI có khóa file; runner chỉ nhận một job tại một thời điểm. Không chạy
nhiều replica runner ghi cùng namespace.

Chạy trực tiếp khi cần chẩn đoán:

```bash
docker compose exec spark-iceberg /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/silver/jobs/silver_pipeline.py --run-id manual-silver-001 \
  --dataset-id faostat_production_coffee
```

Lặp `--dataset-id` để chọn nhiều nguồn; bỏ flag để chạy toàn bộ. Dùng run ID mới
cho một lần replay mới. HTTP request cùng run ID/danh sách dataset nhận lại cùng
job ID; job thất bại không tự biến thành một lần chạy mới. Log ở
`/tmp/silver-jobs/<job_id>/spark.log`, kết quả `results.json`, exit code `exit.json`;
volume `silver-job-logs` giữ log sau khi recreate container. API nội bộ không có
authentication; chỉ dùng mạng tin cậy, thêm authentication trước khi expose.

## Bảng, khóa và partition

Sáu bảng đều partition `years(period_start)`; ba bảng điều khiển không partition.
Không tạo bảng Gold hoặc surrogate key. [DICTIONARY.md](DICTIONARY.md) liệt kê
schema, khóa và ý nghĩa; [baseline.json](baseline.json) là profile read-only của
26 file landing, dùng kiểm tra schema/code coverage và lỗi nguồn. Baseline không
được dùng làm input thay cho Bronze.

`processing_state` ghi dataset/checksum/transform-version/run, snapshot và thống kê.
`dq_results` ghi audit, threshold, row accounting, schema fingerprint, period range,
metadata warning, missing ratio và sentinel count theo metric NASA.
`quarantine_records` giữ raw payload, lineage, mã lỗi và phiên bản để replay.
Quarantine lưu unique payload/error; nếu raw có nhiều dòng lỗi giống hệt nhau,
DQ vẫn đếm mọi occurrence, số dòng quarantine có thể nhỏ hơn số lỗi đầu vào.

`transform_version` là digest của code/config Silver cùng dependencies/Dockerfile.
Đổi rules/mapping làm phiên bản mới; cùng checksum và transform-version chỉ skip
khi SUCCESS gần nhất vẫn có snapshot trong ancestry và source scope khớp phiên bản,
số dòng. SUCCESS cũ trước một phiên bản khác không làm bỏ qua thao tác hoàn nguyên.

Publication dùng một MERGE atomic: cập nhật/thêm khóa của dataset hiện tại và xóa
khóa đã biến mất trong cùng transaction. Không overwrite toàn bảng. Bảng canonical
chứa phiên bản hiện hành của mỗi dataset; phiên bản trước còn trong immutable
Bronze và Iceberg snapshot, không cộng dồn nhiều version vào cùng bảng hiện hành.
MERGE dựa trên [Iceberg Spark writes](https://iceberg.apache.org/docs/1.8.1/spark-writes/).

Schema evolution chỉ tự thêm cột optional sau compatibility check. Removal, rename,
đổi type và nullable thành required bị chặn; không bật merge-schema toàn cục.
Schema input thay đổi phải cập nhật `config/sources.json` có review trước khi chạy.

## DQ, mapping và các quyết định nghiệp vụ

- Missing giữ null; zero thật vẫn là zero. NASA `-999` và biến thể thành null.
- Chuyển nghìn ha/tấn, triệu USD, kg và nghìn bao 60 kg theo `rules.json`.
  Giữ `raw_value`, `unit_raw`, `unit_rule`, `conversion_factor`, raw payload.
  Kiểm tra sai lệch conversion trên từng observation trước khi publish.
- `solar_radiation` giữ giá trị nguồn; file CSV không kèm unit metadata. Cần xác minh
  đơn vị bức xạ trước khi sử dụng metric này trong mô hình hay quy đổi.
- Không đổi VND/LCU/SLC/USD. LCU và SLC là nhãn tiền tệ nguồn, chưa là currency
  identity đủ để làm FX; không cộng chéo quốc gia hoặc chế độ tiền tệ.
- Geography lấy code 63 tỉnh của NASA, alias exact được review trong config.
  Không tự quy đổi sang địa giới mới; tỉnh/huyện/edition vẫn là khóa riêng.
- ISO3/M49 hiện hành dùng pycountry đã pin. Mã lịch sử FAO giữ nguyên; China FAO
  159 và các vùng thống kê Comtrade 251/490/579/699/757/842/899 được giữ bằng mã
  có namespace, không giả tạo một ISO3 hoặc M49 tương đương. Cần crosswalk được
  duyệt nếu muốn đối soát các aggregate này với nguồn khác.
- `X` của FAO (external organization) được gắn estimated theo quy tắc bảo thủ;
  flag gốc vẫn trong payload. Không coi đây là chứng nhận official.
- Niên vụ giữ `season_label`; `period_start/end` là bao lịch hai năm, không phải
  khẳng định vụ cà phê bắt đầu ngày 1/1. Cờ `season_calendar_envelope` ghi rõ điều này.
- UN Comtrade chỉ xuất dòng tổng phương thức vận tải khi nguồn có tổng tương ứng.
  436 dòng annual và 1.636 dòng monthly chi tiết được audit là excluded để tránh
  đếm đôi. Nếu thiếu tổng, dòng chi tiết bị quarantine thay vì bị loại âm thầm.
  Dùng netWgt; FOB ưu tiên trước primaryValue, lưu `value_source_field`, các flags.
  Không suy diễn dữ liệu cao su hoặc hồ tiêu monthly còn thiếu.
- Pink Sheet chỉ dùng Monthly Prices và bốn series coffee/rubber; xác minh header
  và unit-row. Metadata/các sheet khác được đếm excluded, không thành market price.
- Exact duplicate lấy một record deterministic bằng hash payload. Mâu thuẫn cùng
  business key mặc định bị quarantine và chặn toàn dataset, bất kể threshold 1%.
- Ngoại lệ versioned `domestic-coffee-price-v1` chỉ áp dụng cho
  `provincial_coffee_average_price`: nếu cùng khóa, cùng `VND/kg`, đúng indicator
  và chênh tối đa 1, giữ giá cao hơn; winner có `quality_flag`/`dedupe_reason`,
  loser vào quarantine mức WARNING. Sáu khóa Đắk Lắk năm 2022 đã được xử lý theo
  rule này. Chênh lớn hơn 1 hoặc sai unit/indicator vẫn chặn publish.
- `source_priority` chưa gán winner. `reconciliation.compare` chỉ hoạt động khi
  caller xác nhận cùng definition, grain và units; cấu hình mặc định ghi NOT_EVALUATED cùng lý do cho bốn source pair; không
  chạy phép so sánh chưa được duyệt. Không tự so FAO green coffee với toàn HS 0901 hoặc producer
  price với international benchmark. Việc duyệt pair vẫn cần thực hiện trước nghiệm thu nghiệp vụ đầy đủ; job đã
  tích hợp ghi DQ đối soát sau khi xử lý các dataset.

Row accounting có hai tầng: input = excluded + source rows; source rows mở rộng
1/2/4 measure theo source contract. Candidate observations = published candidates
+ quarantine occurrences (gồm warning loser đã giải quyết) + duplicates removed.
Explicit missing nằm trong valid
output và được đếm riêng; không cộng hai lần. Nếu một source row không parse được,
toàn bộ row được quarantine một lần thay vì tạo measure giả. Khi audit FAIL,
`output_rows` trong DQ là số candidate hợp lệ, không phải số đã publish.

## Recovery và giới hạn triển khai

1. Xem `dq_results`, processing state và job log; phân biệt lỗi contract và lỗi hạ tầng.
2. Với unknown mapping/schema, sửa config có review, chạy test, dùng run ID mới.
3. Với lỗi dữ liệu, sửa tại upstream rồi chạy Bronze ingestion như quy trình hiện có;
   Silver chỉ đọc version immutable. Không chỉnh object Bronze bằng tay.
4. Với lỗi sau publish nhưng trước ghi SUCCESS, rerun có thể MERGE lại đúng scope;
   business rows không trùng nhưng có thể tạo snapshot mới và processed timestamp mới.
   Domain table và ba control tables không có transaction chung.
5. Runner restart giữa job: task monitor có thể thất bại; trigger DAG mới để replay.
   Không xóa SUCCESS/state thủ công để ép chạy; dùng versioned change rõ ràng.
6. Không expire snapshot dùng cho skip/time travel mà chưa có retention/recovery policy.

Đây là implementation đã kiểm thử trên stack phát triển hiện có. Spark local mode
và staging file cùng container không hỗ trợ remote executors; cần shared filesystem
hoặc S3A reader và kiểm thử lại để chuyển distributed cluster. Bảo đảm đủ đĩa tạm
cho CSV/cache và đủ RAM cho Spark, Trino, Airflow chạy đồng thời. Image Iceberg REST
fixture hiện hữu cần được thay hoặc cấu hình catalog lưu bền và backup trước khi
coi đây là triển khai production có khả năng khôi phục. Không tự di chuyển catalog
đang có trong thay đổi này. Các lần integration dùng bucket `silver-test-fixtures`
và namespace `silver_test_*`, giữ lại để audit; dọn có chọn lọc theo retention.

Smoke test `lakehouse_demo.py`, dbt `silver.agri_trade`, Gold và Bronze service được
giữ nguyên. Consumers phải chuyển sang sáu contract mới một cách chủ động.
