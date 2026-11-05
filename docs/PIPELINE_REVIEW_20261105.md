# Đối chiếu Bronze → Silver → Gold ngày 05/11/2026

## Kết luận từ mã nguồn

Báo cáo 04/11/2026 trong `docs/PIPELINE_RELEASE_AUDIT_20261104.md` đã ghi nhận
các sửa đổi về Bronze A → B → A, manifest 26 nguồn, Silver policy fingerprint,
candidate/pointer, Gold scope/temporal/candidate và E2E Airflow. Các thay đổi
đó còn trong commit `af57f83`. Vì vậy không lặp lại hoặc thay đổi các quyết
định nghiệp vụ chưa có chứng cứ.

Trong lần rà soát này có thêm bốn vấn đề thực tế:

1. `lakehouse_release` có lịch hằng ngày nhưng DAG Bronze của metadata Airflow
   hiện tại bị paused. Release 05/11 kẹt ở Bronze `queued` cho tới khi unpause;
   sau đó Bronze hoàn thành. Compose trước đây cũng tạo DAG mới ở trạng thái
   paused. Cấu hình đã đổi và runbook ghi rõ cách xử lý metadata cũ.
2. Gold DQ dùng phủ định và phép so sánh SQL ba giá trị, nên `NULL` ở
   `is_canonical`, cả hai trường giá trống, hoặc input/output quy đổi tồn kho
   trống có thể không bị tính là lỗi. Gate đã sửa để chặn các trường hợp này.
   Cần build Gold candidate mới vì transform fingerprint thay đổi.
3. Gold publish trước đây giả định luôn có batch Gold legacy. Catalog đang chạy
   chỉ có schema Silver nên lần phát hành Gold đầu tiên sẽ lỗi ngay lúc tạo
   manifest hoặc pointer. Đã thêm bootstrap namespace metadata và nhánh
   khởi tạo pointer thế hệ 0 sau DQ/view gate; khi đã có pointer, các batch sau
   tiếp tục dùng generation guarded update.
4. Backup độc lập chưa có lệnh vận hành. `scripts/backup_lakehouse.sh` tạo archive
   nhất quán khi Compose dừng, lưu ba volume dữ liệu và toàn bộ raw local, rồi
   kiểm SHA256 và tar. Script từ chối release đang chạy; kiểm tra chốt này đã
   thành công trong run 05/11. `verify_backup_restore.py` đã khôi phục cả bốn
   archive vào volume tạm và so khớp từng file. Chưa thử ứng dụng trên một
   Compose project tách biệt.

## Bằng chứng môi trường hiện tại

- 63 host tests chạy thành công, 11 skip do host thiếu PySpark; `compileall`,
  `docker compose config`, `git diff --check` và `bash -n` đều qua.
- Stack đã được khởi động bằng image sẵn có, giữ nguyên volume. Airflow không
  có import error. Healthcheck theo lịch đã chạy `success` và được unpause.
- Catalog hiện tại chỉ có các schema Silver; không có `gold_semantic` hay
  `gold_meta` tại lúc kiểm tra. Do đó số liệu Gold lịch sử trong báo cáo 04/11
  không chứng minh trạng thái phục vụ của volume đang chạy.
- Release theo lịch 05/11 đã bắt đầu. Bronze run
  `pipeline-bronze-cbf6d67632b51c6b611719d9` hoàn thành sau khi mở DAG.
  Silver run `pipeline-silver-cbf6d67632b51c6b611719d9` đã qua cả mười
  stage và publish `silver-6c5f89c990acfca8b9861ca5` ở generation 1:
  53 PASS, hai WARN, không có FAIL. Gold đã qua inspect, foundation,
  dimensions và facts. Gold `gold-b246c2c7d90716e995738da0` đã publish ở
  pointer generation 0: 222 PASS, 0 FAIL, 710.093 lineage. Trino đọc được
  9.262 dòng KPI production và 22.008 dòng KPI export. DAG E2E kết thúc
  `success` lúc 10:08 giờ Việt Nam.
- Sau E2E, backup `/tmp/tlcn-lakehouse-backup-20261105` (637 MB) gồm ba
  volume dữ liệu và raw local đã qua SHA256; bốn archive được extract vào
  volume tạm rồi so khớp toàn bộ file. Sau restart, Trino vẫn đọc Gold
  pointer generation 0; Airflow không có import error.
- Gold run độc lập `manual__gold_second_20261105` đã publish
  `gold-e26e04d1825ac4f82224247c`, chuyển pointer generation 0 → 1.
  Vì input Silver không đổi, KPI production và export vẫn lần lượt 9.262 và
  22.008 dòng. Rollback về `gold-b246c2c7d90716e995738da0` thành công;
  pointer hiện ở generation 2, KPI production vẫn 9.262 dòng. Archive trước
  lần chạy thứ hai ghi trạng thái generation 0.
- Backup cuối `/tmp/tlcn-lakehouse-backup-20261105-final` (716 MB) được tạo
  sau rollback và ghi trạng thái generation 2. Thư mục và file đều chỉ chủ
  sở hữu đọc được. Cả bốn archive đã được restore vào volume tạm, đối chiếu
  SHA256 từng file; sau restart Trino vẫn đọc pointer generation 2 và KPI.

## Ranh giới dữ liệu và nguồn xác minh

- File NASA không giữ request gốc, nhưng năm cặp tọa độ/ngày từ 2000 đến 2025
  khớp chính xác API NASA POWER với `community=AG`, `time-standard=UTC` và
  đơn vị `MJ/m²/day`; RE, SB và LST không khớp. Bằng chứng và URL tái lập:
  `docs/silver/NASA_POWER_UNIT_VERIFICATION_20261105.md`. Parser và Silver
  stage/core đã được cập nhật; giá trị thiếu của nguồn vẫn NULL.
- Danh mục hành chính của Cục Thống kê có tìm kiếm theo thời điểm. Crosswalk
  huyện phải gắn ngày hiệu lực và khớp tên, tỉnh nguồn trước khi chấp nhận;
  đặc biệt không dùng mã hành chính 2025 cho quan sát 2022. Nguồn:
  https://danhmuchanhchinh.nso.gov.vn/HDSD_DMHC_Web.htm và
  https://danhmuchanhchinh.nso.gov.vn/Default.aspx.
- UN Comtrade phân biệt phiên bản HS, thủ tục hải quan, mode vận tải và đơn vị
  lượng. Tập raw hiện có các trường này nhưng chưa có mapping sản phẩm và
  quy tắc ưu tiên được duyệt, nên giữ Comtrade ngoài canonical. Nguồn:
  https://comtradeapi.un.org/files/v1/app/wiki/MethodologyGuideforComtradePlus.pdf.

## Việc còn cần bằng chứng runtime

Backup cuối phải được sao chép tới nơi lưu trữ độc lập và restore vào một Compose
project riêng để đọc lại pointer/snapshot/KPI. Không công bố production-ready
trước các bước này. REST catalog hiện vẫn là SQLite fixture; monitoring mới
phát hiện trạng thái service trong Airflow, chưa có cảnh báo tập trung hay SLO
được chứng minh.

## Kết quả phát hành bổ sung ngày 05/11

- Ba lần chạy `lakehouse_release` liên tiếp sau rà soát đều `success`, thay đổi
  policy và snapshot Silver qua từng lần. Batch Silver cuối
  `silver-f502510c8b3fdb9a99a1fd4e` (pointer generation 4,
  `source-local-v6-7d813f7c33932fa92d56`) có 55 PASS, 1 WARN, 0 FAIL.
- Workbook giá chứa hai khối tháng 2022; 6 nhóm Đắk Lắk lệch đúng 1 VND/kg.
  Quy tắc giới hạn chọn giá lớn hơn, lưu đủ WINNER/LOSER và sáu conflict
  `RESOLVED`; các khác biệt lớn hơn vẫn bị chặn. Gold phục vụ 456 giá địa
  phương qua `fact_price` với khóa tỉnh `as_published` và lineage.
  Bằng chứng: `docs/silver/PRICE_CONFLICT_20261105.md`.
- HS 0901/0904 là heading rộng hơn commodity nông nghiệp tương ứng. Tám bridge
  đã được phân loại `NOT_EQUIVALENT`, trọng số NULL; không còn `PENDING` và
  không phân bổ giả. Nguồn: https://unstats.un.org/unsd/classifications/Econ/Detail/EN/32/0901
  và https://unstats.un.org/unsd/classifications/Econ/Detail/EN/2089/0904.
- Silver stage/core có 614.754 dòng thời tiết, 609.714 giá trị bức xạ nguồn
  khác NULL đã được chuẩn hóa bằng phép đồng nhất đơn vị, 0 sai lệch; 5.040
  NULL nguồn vẫn NULL. Gold phục vụ 609.714 giá trị bức xạ trong 614.628
  dòng Fact thời tiết có ít nhất một thước đo hợp lệ.
- Gold `gold-bb571a769c0dafc11d4f12a9` sau thay đổi NASA có 223 PASS,
  0 FAIL, 710.549 dòng lineage. Gold tiếp theo
  `gold-ce3178acee2b6d6da72ab21a` dùng router semantic chỉ một release,
  pointer generation 6. KPI production/export vẫn là 9.262/22.008 dòng;
  truy vấn KPI production không còn cảnh báo 89 stage. Đã rollback về batch
  trước rồi trở lại batch mới, KPI tiếp tục đọc được; pointer generation 8.
- Backup cuối `/tmp/tlcn-lakehouse-backup-20261105-current` gồm catalog,
  MinIO, PostgreSQL và raw; SHA256/tar đều qua. Cả bốn archive đã restore vào
  volume tạm và so từng file. Sau restart, pointer Gold generation 8 và KPI
  production 9.262 dòng vẫn đọc được.

## Giới hạn chưa đóng

Batch Gold hiện công bố bảy nhóm giới hạn: Comtrade 21.262 quan sát chưa có
Fact HS nguồn riêng; 2.142 quan sát huyện vẫn dùng source identity và 44 alias
chưa có crosswalk thời gian trong mô hình; 406 quan sát thương mại
`NATIONAL_REPORTED` không tương đương `SOURCE_TOTAL`; 16 dòng EU aggregate
không có mã quốc gia M49; 26 xung đột diện tích và tám xung đột sản lượng tỉnh
giữa các nguồn chưa có quy tắc ưu tiên có chứng cứ. Trong số đó, chênh lệch
sản lượng 2024 có thể tới hàng chục nghìn tấn nên không được tự chọn bằng
quy tắc làm tròn. Các giới hạn này không làm sai các KPI trong phạm vi đã
khai báo, nhưng ngăn kết luận toàn hệ thống production-ready hoặc phân tích
toàn bộ dữ liệu sau Gold.

API `DanhMucQuanHuyen` của Cục Thống kê đã được truy vấn tại ngày 31/12 mỗi
năm 2015–2024 cho bốn tỉnh nguồn. Tên của 43/44 alias có một mã nhất quán
trong các năm quan sát; riêng Đạ Huoai dùng mã 681 trước 2024 và mã 682 vào
cuối 2024 sau sắp xếp. Đạ Tẻh cũng dùng mã 682 trước đó. Vì bảng alias hiện
không có khóa năm hiệu lực, cập nhật một mã duy nhất cho Đạ Huoai sẽ gán sai
lịch sử; dữ liệu huyện tiếp tục dùng khóa nguồn cho tới khi có crosswalk theo
thời gian và xác định ranh giới áp dụng cho số liệu cả năm 2024. API nguồn:
https://danhmuchanhchinh.nso.gov.vn/DMDVHC.asmx?op=DanhMucQuanHuyen.

## Bổ sung thiết kế 8 Fact và phát hành thương mại

Sau phản hồi của chủ dự án, hợp đồng Gold tiếp tục có đúng **8 Fact**. Hai Fact
thương mại hiện có được mở rộng theo ngữ nghĩa nguồn:

- `fact_trade_total` thêm `statistical_scope_code`. Gold đang phục vụ 22.044
  dòng `SOURCE_TOTAL` và 203 dòng `NATIONAL_REPORTED`; hai nhóm có view riêng.
  KPI xuất khẩu cũ vẫn chỉ dùng `SOURCE_TOTAL` và có 22.008 dòng.
- `fact_trade_partner` thêm mã đối tác nguồn, partner2, customs, mode vận tải,
  statistical scope và valuation basis. Có 2.960 dòng FAOSTAT
  `AS_PUBLISHED` và 10.631 dòng Comtrade HS `PRIMARY_VALUE_USD`. View
  `trade_hs_total_mode_country` chỉ có 7.661 dòng ở mode tổng, đối tác quốc
  gia được ánh xạ; dữ liệu HS không được gắn giả vào commodity COFFEE/PEPPER.

Silver mới `silver-f92f05adf0126eaa74763042` chọn 21.231 quan sát Comtrade
không thiếu và loại 31 quan sát thiếu. Gold
`gold-6d7ac0c1e3c4e507d13a7079` ở pointer generation 13 có 223 PASS,
0 FAIL. Lần nâng schema đầu bị chặn tại Trino vì view chuyển release dùng
`UNION` giữa schema cũ và mới; code publication hiện chiếu cột theo tên và
điền scope lịch sử đúng hợp đồng cũ, rồi candidate kế tiếp phát hành thành công.
Candidate 10 Fact ban đầu chưa từng được publish.
Đường rollback đã được kiểm tra hai chiều qua release Gold cũ và trở lại:
KPI `SOURCE_TOTAL` vẫn 22.008 dòng, view `NATIONAL_REPORTED` là 0 ở
release cũ và 203 ở release mới, view HS country total mode trở lại 7.661.

Ngoại lệ Gold còn năm nhóm: 2.142 quan sát huyện theo source identity,
44 alias huyện chưa có crosswalk theo thời gian, 16 dòng EU aggregate,
26 xung đột diện tích tỉnh và tám xung đột sản lượng tỉnh. Catalog SQLite
fixture và backup chưa ở nơi lưu trữ độc lập vẫn là rủi ro triển khai.

## Phát hành đối chiếu huyện và phạm vi phân tích hiện tại

- Crosswalk huyện theo tỉnh, tên nguồn và năm quan sát được lưu tại
  `spark/gold/config/nso_district_crosswalk.json`, đối chiếu API danh mục huyện
  của Cục Thống kê vào ngày 31/12 từng năm. 295/296 khóa tên–năm được xác
  minh; Đạ Huoai 2024 chưa thể gán một mã vì số liệu là cả năm, trong khi
  địa giới thay đổi trong năm. Gold DQ chặn hàng huyện mới thiếu crosswalk.
- Gold `gold-4e7c029665c8969015748a0d` đã publish ở pointer generation 14,
  225 DQ PASS, 0 FAIL. View
  `gold_semantic.fact_production_provincial_district_official` phục vụ 705/708
  dòng Fact huyện, 41 mã huyện chính thức; ba dòng Fact Đạ Huoai 2024 giữ ở
  Fact nguồn. Ngoại lệ huyện giảm còn một khóa tên–năm chưa giải quyết.
- KPI production 9.262 dòng, export SOURCE_TOTAL 22.008 dòng và HS tổng theo
  đối tác quốc gia 7.661 dòng, không đổi so với release trước. Gold còn bốn
  nhóm ngoại lệ: một khóa huyện/năm, 16 hàng EU aggregate, 26 nhóm diện tích
  và tám nhóm sản lượng tỉnh xung đột.
- Với xung đột tỉnh, dữ liệu nguồn vẫn ở `fact_production_provincial` theo
  `record_source_code`; view canonical trả NULL cho riêng thước đo xung đột.
  Chênh lệch RUBBER 2023 và COFFEE 2024 quá lớn để chọn một nguồn hoặc coi là
  sai khác làm tròn mà không có bằng chứng độc lập.
- EU aggregate không phải quốc gia M49; 16 hàng được loại khỏi phạm vi Fact
  tồn kho quốc gia theo hợp đồng đã công bố. Không gán mã quốc gia giả.
- Catalog chạy thực tế vẫn là REST fixture với SQLite. Apache Polaris với
  PostgreSQL là đích phù hợp, nhưng chưa chuyển catalog: công cụ di trú có
  thao tác xóa đăng ký nguồn, còn đăng ký hai catalog rồi tiếp tục ghi ở cả hai
  có thể làm hỏng bảng. Chỉ chuyển khi có quy trình dừng ghi, di trú snapshot
  và view, xác minh Spark/Trino rồi cắt sang một catalog duy nhất. Trạng thái
  hiện tại **sẵn sàng phân tích theo phạm vi Gold đã công bố**, chưa đủ chứng
  cứ để gọi là triển khai production toàn diện. Backup không nằm trong phạm
  vi xử lý mà chủ dự án yêu cầu lần này.

Metabase sau này kết nối với Trino và không phụ thuộc việc dùng Polaris. Sau
khi chủ dự án hỏi về sự cần thiết của Polaris, phần thử nghiệm đã được dừng;
Spark, Trino và Airflow tiếp tục dùng catalog REST hiện hữu. Không có thay đổi
đường đọc Gold hay số liệu KPI do lần thử catalog. Việc thay catalog là một
công việc vận hành riêng nếu sau này cần chịu tải ghi đồng thời và SLO production.
