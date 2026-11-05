# Quy tắc giải quyết 6 giá cà phê Đắk Lắk trùng kỳ

Nguồn: `data/raw/provincial/coffee/gia_ca_phe_nhan_xo_trung_binh_tay_nguyen.xlsx`,
sheet `data`, dòng 38–61. Workbook chứa hai khối tháng 1–12/2022 cho cùng
Đắk Lắk và Lâm Đồng. Giá Lâm Đồng trùng nhau ở mọi tháng. Giá Đắk Lắk
khác nhau đúng 1 VND/kg ở sáu tháng:

| Tháng 2022 | Khối đầu | Khối sau | Chênh lệch VND/kg |
| --- | ---: | ---: | ---: |
| 01 | 40218 | 40217 | 1 |
| 02 | 41026 | 41025 | 1 |
| 06 | 42616 | 42615 | 1 |
| 07 | 42307 | 42306 | 1 |
| 11 | 40337 | 40336 | 1 |
| 12 | 40798 | 40797 | 1 |

Silver giữ nguyên mọi quan sát nguồn và dùng quy tắc phiên bản
`domestic-coffee-price-v1`: chỉ với dataset giá cà phê địa phương, metric
`DOMESTIC_COFFEE_PRICE`, đơn vị `VND/kg`, cùng observation set và cùng grain,
chọn giá lớn hơn nếu biên độ toàn nhóm không quá 1 VND/kg. Nhóm có chênh lệch
lớn hơn, đơn vị khác hoặc metric khác vẫn bị chặn. Bảng
`silver_meta.canonical_conflict` lưu cả hai giá và trạng thái `RESOLVED`, bảng
`canonical_decision` lưu đúng một WINNER và một LOSER mỗi nhóm. Quy tắc này
không suy đoán giá mới hay lấy trung bình của hai khối.
