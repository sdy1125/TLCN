# Silver data dictionary

Schema authoritative: `spark/silver/contracts.py` và `config/contracts.json`.
Tất cả domain tables trong Spark `lakehouse.silver`, Trino `iceberg.silver`.
Trino có thể hiển thị tên các flag camelCase thành lowercase; so schema không phân biệt hoa/thường.
Partition tất cả sáu bảng: `years(period_start)`; Iceberg format-version 2.

| Contract | Số nguồn | Business key |
| --- | ---: | --- |
| production_national | 3 | dataset_id, country_code, commodity_code, year, measure_code |
| production_provincial | 3 | dataset_id, geography_version, geographic_level, province_code, district_name, commodity_code, year, measure_code, edition |
| market_indicator | 10 | dataset_id, geo_type, geo_code, commodity_code, period_start, frequency, indicator_code, market_type |
| trade_partner | 5 | dataset_id, reporter_code, partner_code, commodity_code, period_start, frequency, flow_code, measure_code |
| trade_total | 4 | dataset_id, reporter_code, commodity_code, period_start, frequency, flow_code, measure_code |
| weather_daily | 1 | dataset_id, weather_point, period_start |

## Semantics chung

- `source_record_id`: SHA-256 từ dataset và khóa nghiệp vụ canonical; ổn định qua rerun.
- `source_system`/`dataset_id`: không deduplicate giữa nguồn. Raw payload giữ metadata nguồn.
- `bronze_path`, checksum, ingestion id: source version immutable; run id và processed timestamp là lineage của lần publish.
- `value_standard`: decimal(28,8); không tự đổi currency. `raw_value` và `unit_raw` giữ dữ liệu nguồn.
- `unit_rule` và `conversion_factor`: quy tắc chuyển đơn vị được áp dụng; WDI unit mặc định theo indicator, không theo trường unit trống.
- `observation_status`: official/provisional/forecast/estimated/missing/not_applicable; mất giá trị giữ null.
- `quality_flag`: thông tin bổ sung như metric NASA thiếu hoặc season calendar envelope.
- `source_priority`/`is_preferred`: null nếu chưa có quyết định ưu tiên có cấu hình.
- `district_name` nullable với level=province; edition/geography default rõ trong config.
- Commodity nullable cho WDI vĩ mô; geography code nguồn aggregate giữ namespace khi không có crosswalk đã duyệt.
- NASA wide metrics là độ C, %, m/s, mm/ngày và solar radiation theo unit nguồn
  chưa được xác minh; raw metrics trong payload. Dataset registry lưu provenance
  `ALLSKY_SFC_SW_DWN` cùng `unit_status=UNVERIFIED`; chi tiết được đưa vào DQ thay
  vì lặp trên từng dòng. `value_standard`/unit scalar không áp dụng cho bảng wide.
- Niên vụ chỉ giữ calendar envelope + season_label; không suy diễn ngày bắt đầu vụ.

## Control tables

- `dq_results`: run_id, dataset_id, bronze_ingestion_id, target_contract, check_name, check_scope, severity, status, observed_value, expected_value, threshold, details_json, checked_at.
- `quarantine_records`: dataset_id, bronze_ingestion_id, bronze_checksum_sha256, source_record_id (fingerprint để replay dòng lỗi), target_contract, error_code, error_message, error_severity, raw_payload_json, detected_at, airflow_run_id, transform_version.
- `processing_state`: state_key, dataset_id, checksum, ingestion id, transform_version, run_id, status (`RUNNING`/`COMMITTED`/`SUCCESS`/`FAILED`), target_contract, snapshot_id, output_rows, details_json, checked_at.
- Ba control tables không partition. Severity INFO/WARNING/FAIL; audit status PASS/WARNING/FAIL và NOT_EVALUATED cho source pair chưa được duyệt.

## production_national

| Field | Type | Required |
| --- | --- | --- |
| dataset_id | string | yes |
| source_system | string | yes |
| source_file | string | yes |
| bronze_path | string | yes |
| bronze_ingestion_id | string | yes |
| bronze_checksum_sha256 | string | yes |
| source_record_id | string | yes |
| airflow_run_id | string | no |
| transform_version | string | yes |
| processed_at | timestamp | yes |
| period_start | date | yes |
| period_end | date | no |
| year | int | yes |
| month | int | no |
| frequency | string | yes |
| season_label | string | no |
| observation_status | string | yes |
| measure_code | string | yes |
| raw_value | string | no |
| unit_raw | string | no |
| unit_standard | string | no |
| quality_flag | string | no |
| raw_payload_json | string | no |
| dedupe_reason | string | no |
| value_standard | decimal(28,8) | no |
| conversion_factor | decimal(28,8) | no |
| unit_rule | string | no |
| source_priority | int | no |
| is_preferred | boolean | no |
| country_code | string | no; see key semantics |
| commodity_code | string | no; see key semantics |
| country_m49 | string | no |
| country_iso3 | string | no |
| country_name_raw | string | no |
| faostat_item_code | string | no |
| element_code | string | no |

## production_provincial

| Field | Type | Required |
| --- | --- | --- |
| dataset_id | string | yes |
| source_system | string | yes |
| source_file | string | yes |
| bronze_path | string | yes |
| bronze_ingestion_id | string | yes |
| bronze_checksum_sha256 | string | yes |
| source_record_id | string | yes |
| airflow_run_id | string | no |
| transform_version | string | yes |
| processed_at | timestamp | yes |
| period_start | date | yes |
| period_end | date | no |
| year | int | yes |
| month | int | no |
| frequency | string | yes |
| season_label | string | no |
| observation_status | string | yes |
| measure_code | string | yes |
| raw_value | string | no |
| unit_raw | string | no |
| unit_standard | string | no |
| quality_flag | string | no |
| raw_payload_json | string | no |
| dedupe_reason | string | no |
| value_standard | decimal(28,8) | no |
| conversion_factor | decimal(28,8) | no |
| unit_rule | string | no |
| source_priority | int | no |
| is_preferred | boolean | no |
| geography_version | string | no; see key semantics |
| geographic_level | string | no; see key semantics |
| province_code | string | no; see key semantics |
| district_name | string | no; see key semantics |
| commodity_code | string | no; see key semantics |
| edition | string | no; see key semantics |
| province_name_raw | string | no |
| province_name_normalized | string | no |
| parse_status | string | no |

## market_indicator

| Field | Type | Required |
| --- | --- | --- |
| dataset_id | string | yes |
| source_system | string | yes |
| source_file | string | yes |
| bronze_path | string | yes |
| bronze_ingestion_id | string | yes |
| bronze_checksum_sha256 | string | yes |
| source_record_id | string | yes |
| airflow_run_id | string | no |
| transform_version | string | yes |
| processed_at | timestamp | yes |
| period_start | date | yes |
| period_end | date | no |
| year | int | yes |
| month | int | no |
| frequency | string | yes |
| season_label | string | no |
| observation_status | string | yes |
| measure_code | string | yes |
| raw_value | string | no |
| unit_raw | string | no |
| unit_standard | string | no |
| quality_flag | string | no |
| raw_payload_json | string | no |
| dedupe_reason | string | no |
| value_standard | decimal(28,8) | no |
| conversion_factor | decimal(28,8) | no |
| unit_rule | string | no |
| source_priority | int | no |
| is_preferred | boolean | no |
| geo_type | string | no; see key semantics |
| geo_code | string | no; see key semantics |
| commodity_code | string | no; see key semantics |
| indicator_code | string | no; see key semantics |
| market_type | string | no; see key semantics |
| country_iso3 | string | no |
| province_code | string | no |
| currency_code | string | no |
| geo_kind | string | no |
| indicator_name_raw | string | no |

## trade_partner

| Field | Type | Required |
| --- | --- | --- |
| dataset_id | string | yes |
| source_system | string | yes |
| source_file | string | yes |
| bronze_path | string | yes |
| bronze_ingestion_id | string | yes |
| bronze_checksum_sha256 | string | yes |
| source_record_id | string | yes |
| airflow_run_id | string | no |
| transform_version | string | yes |
| processed_at | timestamp | yes |
| period_start | date | yes |
| period_end | date | no |
| year | int | yes |
| month | int | no |
| frequency | string | yes |
| season_label | string | no |
| observation_status | string | yes |
| measure_code | string | yes |
| raw_value | string | no |
| unit_raw | string | no |
| unit_standard | string | no |
| quality_flag | string | no |
| raw_payload_json | string | no |
| dedupe_reason | string | no |
| value_standard | decimal(28,8) | no |
| conversion_factor | decimal(28,8) | no |
| unit_rule | string | no |
| source_priority | int | no |
| is_preferred | boolean | no |
| reporter_code | string | no; see key semantics |
| partner_code | string | no; see key semantics |
| commodity_code | string | no; see key semantics |
| flow_code | string | no; see key semantics |
| reporter_m49 | string | no |
| reporter_iso3 | string | no |
| partner_m49 | string | no |
| partner_iso3 | string | no |
| hs_code | string | no |
| faostat_item_code | string | no |
| is_estimated | boolean | no |
| isNetWgtEstimated | boolean | no |
| isReported | boolean | no |
| isAggregate | boolean | no |
| isQtyEstimated | boolean | no |
| isAltQtyEstimated | boolean | no |
| isGrossWgtEstimated | boolean | no |
| value_source_field | string | no |
| classification_code | string | no |
| transport_code | string | no |

## trade_total

| Field | Type | Required |
| --- | --- | --- |
| dataset_id | string | yes |
| source_system | string | yes |
| source_file | string | yes |
| bronze_path | string | yes |
| bronze_ingestion_id | string | yes |
| bronze_checksum_sha256 | string | yes |
| source_record_id | string | yes |
| airflow_run_id | string | no |
| transform_version | string | yes |
| processed_at | timestamp | yes |
| period_start | date | yes |
| period_end | date | no |
| year | int | yes |
| month | int | no |
| frequency | string | yes |
| season_label | string | no |
| observation_status | string | yes |
| measure_code | string | yes |
| raw_value | string | no |
| unit_raw | string | no |
| unit_standard | string | no |
| quality_flag | string | no |
| raw_payload_json | string | no |
| dedupe_reason | string | no |
| value_standard | decimal(28,8) | no |
| conversion_factor | decimal(28,8) | no |
| unit_rule | string | no |
| source_priority | int | no |
| is_preferred | boolean | no |
| reporter_code | string | no; see key semantics |
| commodity_code | string | no; see key semantics |
| flow_code | string | no; see key semantics |
| reporter_m49 | string | no |
| reporter_iso3 | string | no |

## weather_daily

| Field | Type | Required |
| --- | --- | --- |
| dataset_id | string | yes |
| source_system | string | yes |
| source_file | string | yes |
| bronze_path | string | yes |
| bronze_ingestion_id | string | yes |
| bronze_checksum_sha256 | string | yes |
| source_record_id | string | yes |
| airflow_run_id | string | no |
| transform_version | string | yes |
| processed_at | timestamp | yes |
| period_start | date | yes |
| period_end | date | no |
| year | int | yes |
| month | int | no |
| frequency | string | yes |
| season_label | string | no |
| observation_status | string | yes |
| measure_code | string | yes |
| raw_value | string | no |
| unit_raw | string | no |
| unit_standard | string | no |
| quality_flag | string | no |
| raw_payload_json | string | no |
| dedupe_reason | string | no |
| value_standard | decimal(28,8) | no |
| conversion_factor | decimal(28,8) | no |
| unit_rule | string | no |
| source_priority | int | no |
| is_preferred | boolean | no |
| weather_point | string | no; see key semantics |
| province_code_63 | string | no |
| province_name | string | no |
| latitude | double | no |
| longitude | double | no |
| temperature_avg_c | double | no |
| temperature_max_c | double | no |
| temperature_min_c | double | no |
| relative_humidity_pct | double | no |
| wind_speed_m_s | double | no |
| precipitation_mm | double | no |
| solar_radiation | double | no |
