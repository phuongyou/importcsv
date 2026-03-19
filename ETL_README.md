# ETL Import Guide — CTA Contract Management System

## Tổng quan

Script `etl_import.py` đọc file Excel gốc và import vào PostgreSQL theo đúng schema v3.0.

### Thứ tự import (đảm bảo FK constraints)
```
business_units → users (seed) → clients → agreements → agreement_insurance → sows → partnerships
```

---

## 1. Cài đặt

```bash
pip install pandas openpyxl psycopg2-binary
```

---

## 2. Cấu hình kết nối DB

Đặt biến môi trường trước khi chạy:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=cta_contracts
export DB_USER=postgres
export DB_PASSWORD=your_password
```

Hoặc tạo file `.env` và load:
```bash
source .env
```

---

## 3. Tạo database và schema trước

```bash
# Tạo DB
psql -U postgres -c "CREATE DATABASE cta_contracts;"

# Chạy DDL schema
psql -U postgres -d cta_contracts -f CTA_PostgreSQL_Schema.sql
```

---

## 4. Chạy ETL

### Dry run — kiểm tra dữ liệu trước, không ghi DB
```bash
python etl_import.py --dry-run
```

**Output mẫu:**
```
QB Contacts rows:         332
Agreements rows (data):   114
SOW rows (data):          199
Partnership rows (data):   12
Unique clients total:     410

DATA QUALITY ISSUES:
  Agreements with corrupt expiry date: 27  ← dates = '00:00:00' → bị bỏ qua, set NULL
  SOWs with no project name:            8  ← dùng tên client làm fallback
```

### Import toàn bộ (một lần)
```bash
python etl_import.py
```

### Import từng bước (nếu cần debug)
```bash
python etl_import.py --sheet clients
python etl_import.py --sheet agreements
python etl_import.py --sheet sows
python etl_import.py --sheet partnerships
```

---

## 5. Những gì script xử lý tự động

| Vấn đề trong Excel | Cách xử lý |
|---|---|
| Expiration date = `00:00:00` (corrupt) | → Set `NULL` |
| Expiration date = `-46084` (Excel date serial lỗi) | → Set `NULL` |
| `Days to Contract End` là số âm | → Bỏ qua (AUTO field, server tính lại) |
| Contract Type không nằm trong ENUM | → Map về `Other` |
| Work Type lạ (`MS Change Order`, `Purchase Order`…) | → Map về ENUM gần nhất |
| `Requires PO` = `Need PO `, `yes`, số PO thật | → Normalize về `Yes`/`No` |
| `Billing Cycle` typo (`Quarerly`) | → Normalize → `Quarterly` |
| Client xuất hiện ở cả QB + Agreements + SOW | → Merge, ưu tiên QB (có địa chỉ đầy đủ hơn) |
| Phone có prefix `Phone: ` | → Strip prefix |
| Client trùng tên khác case/dấu cách | → `ON CONFLICT (company_name) DO UPDATE` — merge data |
| Managed Services / Retainer SOW | → Tự set `renewal_required = Yes` theo business rule |
| SOW đã có `Signed Copy = Yes` | → Set `workflow_status = Executed` |
| SOW chưa ký | → Set `workflow_status = Pre-Sales Draft` |

---

## 6. Validation Report (tự động sau khi import)

Script in ra sau khi chạy:
```
==============================
POST-IMPORT VALIDATION REPORT
==============================
  business_units                     4 rows
  users                              1 rows
  clients                          410 rows
  agreements                       114 rows
  agreement_insurance               25 rows
  sows                             199 rows
  partnerships                      12 rows

--- Data Quality Checks ---
  Agreements with missing client FK:  0
  SOWs with missing client FK:        0

  Agreements by RAG:
    G: 87
    R: 27

  SOWs by status:
    Active:   141
    Archived:  45
    Expired:   13
```

---

## 7. Sau khi import — cần làm thủ công

### 7.1 Tạo real users
```sql
INSERT INTO users (username, password_hash, full_name, email, role, business_unit_id)
VALUES
  ('brittney.ivison', '$2b$12$...', 'Brittney Ivison', 'brittney@viscosityna.com', 'contract_manager', 1),
  ('admin.user',      '$2b$12$...', 'Admin User',       'admin@viscosityna.com',    'admin',            NULL);
```

### 7.2 Recalculate RAG và days_to_end
```sql
-- Cập nhật days_to_end và rag_status cho agreements
UPDATE agreements SET
  days_to_end = CASE
    WHEN expiration_date IS NULL THEN NULL
    ELSE (expiration_date - CURRENT_DATE)::INT
  END;

UPDATE agreements SET
  rag_status = CASE
    WHEN status IN ('Expired','Terminated','Archived') THEN 'R'
    WHEN expiration_date IS NULL THEN 'G'
    WHEN days_to_end < 0 THEN 'R'
    WHEN days_to_end <= 180 THEN 'A'
    ELSE 'G'
  END;

-- Cập nhật SOWs tương tự
UPDATE sows SET
  days_to_end = CASE
    WHEN expiration_date IS NULL THEN NULL
    ELSE (expiration_date - CURRENT_DATE)::INT
  END;

UPDATE sows SET
  rag_status = CASE
    WHEN status IN ('Expired','Terminated','Archived') THEN 'R'
    WHEN expiration_date IS NULL THEN 'G'
    WHEN days_to_end < 0 THEN 'R'
    WHEN days_to_end <= 180 THEN 'A'
    ELSE 'G'
  END;
```

### 7.3 Link SOWs với parent agreements
```sql
-- Ví dụ: link theo client_id (cần review thủ công từng trường hợp)
-- Script gợi ý để review:
SELECT
  s.id AS sow_id,
  s.project_name,
  s.client_id,
  c.company_name,
  a.id AS agreement_id,
  a.contract_type
FROM sows s
JOIN clients c ON s.client_id = c.client_id
LEFT JOIN agreements a ON a.client_id = s.client_id AND a.contract_type = 'MSA'
WHERE s.parent_agreement_id IS NULL
ORDER BY c.company_name;
```

### 7.4 Mark SOWs có renewal vào subscription tracker
```sql
UPDATE sows SET in_subscription_tracker = 1
WHERE renewal_required = 'Yes'
  AND work_type IN ('Managed Services','Retainer','Remote Support');
```

### 7.5 Reset sequence IDs sau ON CONFLICT
```sql
SELECT setval('clients_id_seq',    (SELECT MAX(id) FROM clients));
SELECT setval('agreements_id_seq', (SELECT MAX(id) FROM agreements));
SELECT setval('sows_id_seq',       (SELECT MAX(id) FROM sows));
SELECT setval('users_user_id_seq', (SELECT MAX(user_id) FROM users));
```

---

## 8. Nếu cần chạy lại (reset)

```sql
-- Xóa data theo thứ tự ngược FK
TRUNCATE TABLE client_discounts, order_documents, renewal_tasks, audit_log,
               rate_history, change_orders, agreement_insurance,
               sows, agreements, partnerships, renewal_contacts,
               vendor_contacts, clients,  business_units ,subscriptions
RESTART IDENTITY CASCADE;
```

Rồi chạy lại script.

---

## 9. Sheets không import (lý do)

| Sheet | Lý do không import |
|---|---|
| `Contracts` | Dữ liệu trùng với `Agreements` + `Statements of Work` (pivot view) |
| `MS Dev Renewals` | Pivot table — không phải raw data |
| `Software \| Licensing Agreements` | Chỉ có 10 rows, thiếu nhiều field — import thủ công sau |
| `Scrum Open Contracts` | Project management data — ngoài scope schema v3 |
| `Scrum Client List` | Subset của QB Contacts đã import |
| `QB Consolidated Contacts` | Đã dùng làm nguồn cho clients |
| `VT Contracts` / `VNA Contracts` | Subset của Agreements sheet |
| `Insurance` | Viscosity's own insurance (không phải client insurance) |
| `SOW Insurance Info` | Reference data, không phải transactional |
| `Audit` / `Audit 11.09` | Notes lịch sử, không cấu trúc — không import |
| `Renewals` | Clause text đã nằm trong agreements.renewal_clause_text |
| `Budgets` | Pivot table — data đã có trong SOWs |
| `Settings & Instructions` | Config/reference — không import |


## 10.check doplicate
clients: Check by company_name
agreements: Check by client_id + document_name + effective_date
sows: Check by client_id + project_name + effective_date
partnerships: Check by company_name