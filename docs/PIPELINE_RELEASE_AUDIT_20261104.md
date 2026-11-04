# Bronze → Silver → Gold: kiểm tra và sửa đổi ngày 04/11/2026

## Phạm vi và bằng chứng

Báo cáo này đối chiếu danh sách vấn đề ngày 04/11/2026 với mã nguồn và lần
chạy Airflow trên môi trường local. Số liệu của batch cũ là bằng chứng lịch sử;
không dùng chúng làm điều kiện nghiệm thu batch mới. Xem
`docs/gold/RELEASE_REPORT_20261104.md` cho kết quả Gold v3 trước lần chạy này.

## Đã sửa trong mã nguồn

| Vấn đề | Thay đổi |
| --- | --- |
| Bronze A → B → A | Tái sử dụng object đã xác minh checksum và cập nhật `latest_success` về A. Có test cho rollback và object hỏng. |
| Bronze thiếu release chung | Manifest bất biến gồm toàn bộ nguồn, URI, checksum, kích thước và fingerprint. Silver chỉ đọc manifest theo `BRONZE_RELEASE_ID`. |
| Drive chỉ xét file ID | Tải file tạm và so SHA256, kiểm MD5 nếu Drive cung cấp, lưu phiên bản local cũ, chặn HTML và XLSX sai định dạng. `-Prune` chuyển file vào kho lưu trữ. Handoff marker chặn Bronze trong lúc tải; manifest SHA256 của cả 26 file được Bronze xác minh trước ingest. |
| Silver chỉ có backfill DAG | `silver_release` điều phối foundation, backfill, repair, refs, policy, canonical, DQ, smoke, prepare và activate. `lakehouse_release` nối Bronze → Silver → Gold. |
| Silver gate theo số đếm cũ | DQ dùng registry và quy tắc dữ liệu, kiểm đủ bộ rule; smoke WDI kiểm tính nhất quán NULL thay vì 138 cố định. WARN được ghi theo batch. |
| Policy thiếu phiên bản input | Policy hash mã xử lý, cấu hình, observation set, snapshot core/ref và fingerprint Bronze. Thay đổi sẽ tạo policy mới và tính lại canonical. |
| Silver RETIRE rồi ACTIVE | Candidate có manifest đầy đủ; publication pointer một hàng được chuyển bằng cập nhật kiểm soát generation. Có lệnh rollback. Gold đọc snapshot đã pin. |
| Trùng nguồn khi MERGE địa lý | Gộp tên nguồn cùng mã địa lý; chặn mã có cấu trúc hành chính mâu thuẫn. |
| Gold coverage, temporal, candidate, DQ | Gold v3 trước đó đã có scope contract, phân loại toàn bộ WINNER, temporal join, candidate riêng, pointer, semantic views và 222 DQ PASS trên batch thử. |
| Gold retry sau lỗi semantic views | Registry giữ candidate ở `PREPARED` cho đến khi views được kiểm tra và pointer đã chuyển. Retry có thể tiếp tục; release dở dang của lần thử trước đã được đổi về `PREPARED`. |
| Trino/Spark đọc pointer cũ | Tắt cache bảng Iceberg ở REST fixture và Spark, cùng metadata cache Iceberg ở Trino. Sau đó rollback Silver B → C được Trino thấy ngay ở generation 6 → 7 mà không restart. |

## Bằng chứng runtime trong lần này

- Airflow import được các DAG mới. Bronze tạo release đầy đủ 26 nguồn và kiểm
  checksum. Lần E2E đầu tiên dừng ở `refs` vì trùng khóa nguồn; sau khi sửa,
  `refs` chạy thành công.
- Silver đã chạy thành công toàn bộ mười stage trên Airflow và chuyển pointer
  sang `silver-8a87d124ed44ef52b98a60f5` (generation 1). DQ của batch này
  có 55 rule: 53 PASS, hai WARN, không có BLOCK FAIL; policy là
  `source-local-v6-52938b7894cca7cbf2ca`.
- Bộ test host: 61 PASS, 11 Spark-dependent SKIP. `git diff --check` sạch.
- Gold E2E lần đầu dừng ở inspect khi SQLite REST catalog trả
  `SQLITE_BUSY_SNAPSHOT`. Sau khi dừng Trino và khởi động lại riêng catalog,
  Silver batch thứ hai đã hoàn tất và Gold đã pin
  `silver-dac84a81cb6e331257e9b223`. Gold build Fact/lineage thành công
  nhưng publish dừng khi Trino tạm tắt; lỗi này phát hiện thứ tự ghi registry
  chưa an toàn và đã được sửa trước lần E2E thành công dưới đây.
- Run `manual__e2e_20261104_fixed` đã hoàn thành `success` qua đủ ba tầng.
  Silver `silver-b2a36b2e5c4a6e6018aabac5` có 53 PASS, hai WARN, 0 FAIL.
  Gold `gold-84bab056530b551e14627720` có 222 PASS, 0 FAIL, 710.093
  lineage và 11 nhóm ngoại lệ; pointer Gold ở generation 10 sau publish.
  Trino đọc KPI production 9.262 dòng và KPI export SOURCE_TOTAL 22.008 dòng.
  Rollback Gold về release trước rồi trở lại release mới thành công; pointer
  Gold ở generation 12. Rollback Silver về batch trước rồi trở lại batch mới
  thành công; sau khi tắt cache, Trino thấy ngay generation 6 và 7.

## Chưa đủ điều kiện đóng

1. **Catalog và backup:** REST fixture dùng SQLite, đã gặp lỗi ghi đồng thời.
   Chưa có bản backup độc lập, phục hồi catalog và warehouse đồng bộ, hay diễn
   tập restore. Cần triển khai catalog bền vững, backup có kiểm tra restore và
   thử lỗi giữa các bước trên môi trường tách biệt. Các credential mặc định
   trong Compose cũng phải được thay và quản lý qua secret store khi triển khai.
2. **Chứng cứ nghiệp vụ:** Comtrade, mã huyện chính thức, tám bridge pending,
   sáu xung đột giá và đơn vị bức xạ NASA cần nguồn xác minh và người duyệt.
   Các hàng này tiếp tục giữ nguyên nguồn, không tự điền giá trị hay trọng số.
3. **Kiểm thử nhiều batch:** Đã có test A → B → A ở Bronze và Gold candidate
   trước đó, ba batch Silver cùng input và hai lần rollback pointer trong lần
   này. Cần chạy batch Silver/Gold mới với input hoặc mapping thay đổi, so sánh
   KPI và tiêm lỗi trước/sau chuyển pointer trên môi trường tách biệt. Cũng
   cần kiểm chứng đầy đủ lớp semantic khi đổi release.
4. **Vận hành:** Chưa có cảnh báo tập trung, SLO, quy trình xử lý sự cố và
   kiểm tra script Drive bằng PowerShell trên môi trường này (`pwsh` chưa cài).
   Script hiện thay từng file sau khi xác minh; marker và checksum toàn bộ
   nguồn chặn Bronze khi cập nhật chưa hoàn tất. Cần kiểm chứng script bằng
   PowerShell và thử tình huống Drive thất bại giữa 26 file trên môi trường
   tách biệt trước khi coi handoff hoàn chỉnh.
5. **Tách biệt Silver candidate:** Manifest pin snapshot và pointer bảo vệ Gold,
   nhưng core/ref/meta vẫn là các bảng ghi chung. Consumer Silver trực tiếp
   phải đọc theo pointer và snapshot; cần tách vùng ghi candidate nếu muốn
   cho phép consumer tùy ý đọc bảng Silver hiện hành trong lúc build.
6. **Hiệu năng semantic views:** KPI production trả kết quả nhưng Trino cảnh
   báo kế hoạch có 53 stage, vượt ngưỡng mềm 50. Các view hợp nhiều release
   cần được tối ưu hoặc rút gọn khi lịch sử phát hành tăng.

Vì các mục trên, môi trường local này chưa thể được gọi là production-ready.
Gold/BI chỉ nên đọc release được pointer tham chiếu; không dùng kết quả
`ACTIVE_WITH_EXCEPTIONS` cho KPI ngoài phạm vi của scope contract.
