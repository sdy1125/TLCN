# NASA POWER: xác minh đơn vị và chuẩn ngày của CSV đã ingest

Ngày kiểm tra: 2026-11-05. Tệp Bronze `nasa_power_weather_daily_63.csv` chỉ
giữ giá trị, tọa độ và ngày; request gốc không còn. Truy vấn lại API chính thức
NASA POWER với `community=AG`, `time-standard=UTC`, tham số
`ALLSKY_SFC_SW_DWN` cho các điểm sau:

| Điểm | Ngày | CSV | API AG UTC | Đơn vị API |
| --- | --- | ---: | ---: | --- |
| An Giang (10.3864, 105.4352) | 2000-01-01 | 17.72 | 17.72 | MJ/m²/day |
| An Giang (10.3864, 105.4352) | 2010-01-01 | 15.02 | 15.02 | MJ/m²/day |
| Đắk Lắk (12.6667, 108.05) | 2020-01-01 | 14.57 | 14.57 | MJ/m²/day |
| Hà Nội (21.0278, 105.8342) | 2000-01-01 | 9.60 | 9.60 | MJ/m²/day |
| Lâm Đồng (11.95, 108.44) | 2025-01-01 | 11.90 | 11.90 | MJ/m²/day |

Ví dụ truy vấn tái lập:

```text
https://power.larc.nasa.gov/api/temporal/daily/point?parameters=ALLSKY_SFC_SW_DWN,T2M&community=AG&longitude=105.4352&latitude=10.3864&start=20000101&end=20000102&format=JSON&time-standard=UTC
```

Ở ngày 2000-01-01, `T2M` trong CSV là 25.69 °C; API UTC trả 25.69,
còn LST trả 25.53. API RE tại cùng ngày trả 4.9217 kWh/m²/day và SB
trả 205.07 W/m², khác hẳn 17.72 của CSV. Năm mẫu AG UTC khớp chính xác.
Vì vậy `solar_native_value` được xác minh có đơn vị MJ/m²/day và được sao
chép trực tiếp sang `solar_radiation_mj_m2_day` khi không phải NULL.

Các giá trị nguồn bị thiếu vẫn là NULL; quy tắc này không nội suy thời tiết.
CSV là một điểm đại diện cho mỗi tỉnh, không phải trung bình không gian tỉnh.
API nguồn: https://power.larc.nasa.gov/docs/services/api/temporal/daily/
