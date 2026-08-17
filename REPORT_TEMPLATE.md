# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đào Hải Đăng  **Lớp:** Chiều E403  AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 102.5s
  run 2/3 … 64.5s
  run 3/3 … 78.4s

  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM:
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp

  Bốn tiêu chí chính: 4/4 đạt.
  Kiểm tra DAG trong file: catchup=False, max_active_runs=1.
  Kiểm tra dashboard EXTRA không thực hiện vì thiếu
  data/gold_events/*.parquet.
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng từ **13.790** lên **26.270** hàng sau khi chạy lại pipeline. Một số `ticket_id` xuất hiện 3 lần, ví dụ `T005063`, `T004016` và `T008176`. |
| **Nguyên nhân** | `gold_training_set` là incremental model có grain 1 hàng / 1 `ticket_id`, nhưng `config()` chưa khai báo `unique_key` và chiến lược ghi. Vì vậy dbt không nhận diện được bản ghi cùng `ticket_id` khi retry/chạy lại và thực hiện `INSERT/append`; các hàng cũ bị ghi thêm thay vì được cập nhật. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. `dags/ai_training_pipeline.py`: đặt `catchup=False` và `max_active_runs=1` để hạn chế chạy bù/chạy đồng thời. Giữ nguyên điều kiện lọc theo `run_date`. |
| **Bằng chứng** | Trước sửa: lượt 1 có **13.790** hàng, lượt 2 có **26.270** hàng; `silver_tickets` có **12.480** hàng và **12.480 ticket duy nhất**, chứng minh source không bị lặp. CDC có bản ghi `op='u'`, nên bảng entity phải merge theo `ticket_id` thay vì append. Sau sửa: chạy pipeline hai lần liên tiếp đều giữ `gold_training_set` ở **12.480** hàng, checksum ba lượt `8dd7c98653` giống nhau và số ticket bị lặp là **0**. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` có **8.645** hàng thay vì **9.100**, thiếu **455** cặp `(event_date, customer_id)`. Các cặp thiếu tập trung ở các ngày cũ từ `2026-08-03` đến `2026-08-13`. |
| **P99 độ trễ đo được** | **2.7258 ngày**; `max = 2.9447` ngày; tỷ lệ dữ liệu đến trễ hơn 1 ngày là **5.05%**. |
| **Lookback đã chọn** | **3 ngày** — bằng `ceil(P99) = ceil(2.7258)`, đủ bao phủ khoảng 99% dữ liệu đến trễ. |
| **Nguyên nhân** | Điều kiện incremental hiện tại chỉ xử lý `event_date > max(event_date)` trong target. Vì vậy event có ngày sự kiện cũ nhưng được nạp muộn không được tính lại. Ví dụ các event ngày `2026-08-03` được nạp trong khoảng `2026-08-04` đến `2026-08-06`. |
| **Cách khắc phục** | Nới incremental filter thành lookback 3 ngày: `event_date >= max(event_date) - interval 3 day`. Vì cùng một cặp `(event_date, customer_id)` được tính lại, khai báo `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'`. |
| **Bằng chứng** | Trước sửa: **8.645** hàng; thiếu **455** cặp. Sau sửa: chạy pipeline hai lần liên tiếp đều có **9.100** hàng, đạt kỳ vọng và không bị cộng dồn do dùng `merge` theo `(event_date, customer_id)`. Checksum ba lượt `3db448685c` giống nhau. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn P99 thay vì `max` vì P99 bao phủ phần lớn dữ liệu trễ nhưng không để một outlier đơn lẻ làm mở rộng cửa sổ. Lookback 3 ngày khiến mỗi lượt chạy đọc lại thêm khoảng 3 ngày dữ liệu và chi phí này phát sinh ở mọi lượt chạy sau đó; đổi lại các bản ghi đến trễ trong khoảng đã đo được sẽ được cập nhật. Dùng `max` sẽ an toàn hơn cho mọi độ trễ quan sát được nhưng tốn chi phí hơn và phụ thuộc vào outlier.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có **6.488 NULL**, cùng với giá trị ngoài contract: `-1` (**32** hàng), `0` (**49** hàng) và `5` (**37** hàng). Tổng số bản ghi không hợp lệ là **6.606**. |
| **Nguyên nhân** | `priority_raw` đổi kiểu/miền giá trị trong chu kỳ: ban đầu là chuỗi số `1..4`, sau đó xuất hiện nhãn chữ từ `2026-08-10` (ngày này có `977/984` bản ghi không phải số). Macro hiện tại chỉ `try_cast` trực tiếp nên các nhãn chữ thành `NULL`; các giá trị số ngoài `1..4` cũng không bị loại đúng theo contract. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Chuỗi số `1..4`: giữ nguyên dưới dạng integer. Nhãn `urgent/high/medium/low`: chuẩn hoá lần lượt thành `1/2/3/4`. Giá trị `P1/P2/unknown/0/5/-1/chuỗi rỗng/NULL`: coi là bản ghi lỗi, trả về `NULL` và đưa vào `quarantine_tickets`. |
| **Cách khắc phục** | Macro `normalize_priority` chuẩn hoá hai nhóm hợp lệ và trả `NULL` cho nhóm lỗi. Lọc `priority_clean is not null` trước bước `row_number()` trong `silver_tickets` để bản ghi lỗi không thể che mất bản ghi hợp lệ mới nhất. `quarantine_tickets` lấy toàn bộ bản ghi có priority không hợp lệ. Bật `contract.enforced: true` và test `not_null`/`accepted_values` cho cột `priority`. |
| **Bằng chứng** | Trước sửa: Silver có `-1=32`, `0=49`, `5=37`, `NULL=6.488`, tổng **6.606** bản ghi lỗi. Sau sửa: `silver_tickets` giữ đủ **12.480** ticket, `quarantine_tickets` có **312** bản ghi lỗi, `priority` chỉ còn miền hợp lệ `1..4`, và `dbt test` đạt **11/11 pass**. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn bản ghi lỗi ở Silver thay vì làm pipeline dừng ở Bronze: Bronze giữ nguyên dữ liệu nguồn để truy vết, còn Silver cung cấp dữ liệu sạch cho downstream và chuyển riêng bản ghi lỗi sang quarantine để xử lý/kiểm tra mà không làm mất toàn bộ lượt chạy.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain và natural key trước khi chọn incremental strategy; kiểm tra rerun có idempotent không. |
| 2 | Đo phân bố độ trễ giữa thời điểm sự kiện và thời điểm ingest, dùng P99 để chọn lookback thay vì đoán. |
| 3 | So sánh domain/schema thực tế với contract, chuẩn hoá giá trị hợp lệ và quarantine bản ghi lỗi thay vì làm dừng pipeline. |
