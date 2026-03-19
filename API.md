# CTA ETL Import Server - API Reference

## Base URL
```
http://localhost:5000
```

---

## Health Check

### GET `/`
Kiểm tra server sống hay không

**Response:**
```json
{
  "status": "ok",
  "service": "CTA ETL Import Server",
  "version": "1.0.0",
  "endpoints": {
    "POST /import": "Upload and import Excel file",
    "POST /import/dry-run": "Parse Excel file without importing",
    "GET /health": "Health check"
  }
}
```

---

## Import File

### POST `/import`
Upload và import file Excel vào database

**Request:**
- Content-Type: `multipart/form-data`
- Fields:
  - `file` (required): Excel file (.xlsx, .xls, .xlsm)
  - `sheets` (optional): Comma-separated sheet names (e.g., "clients,agreements,sows")

**Response (Success):**
```json
{
  "status": "imported",
  "file": "Viscosity_Master_Contracts_File_-_CTA.xlsx",
  "timestamp": "2026-03-18T10:30:45.123456",
  "dry_run": false,
  "steps": {
    "business_units": {
      "status": "completed",
      "message": "Completed business_units"
    },
    "system_user": {
      "status": "completed",
      "message": "Completed system_user"
    },
    "clients": {
      "status": "completed",
      "message": "Completed clients"
    },
    "agreements": {
      "status": "completed",
      "message": "Completed agreements"
    },
    "agreement_insurance": {
      "status": "completed",
      "message": "Completed agreement_insurance"
    },
    "sows": {
      "status": "completed",
      "message": "Completed sows"
    },
    "partnerships": {
      "status": "completed",
      "message": "Completed partnerships"
    }
  }
}
```

**Response (Error):**
```json
{
  "status": "error",
  "message": "Invalid or corrupted Excel file: ...",
  "traceback": "..."
}
```

**HTTP Status Codes:**
- `200` - Import successful
- `400` - Bad request (invalid file, missing file, etc.)
- `413` - File too large (max 50MB)
- `500` - Server error

---

## Dry Run (Validation Only)

### POST `/import/dry-run`
Validate file mà không import vào database

**Request:**
- Content-Type: `multipart/form-data`
- Fields:
  - `file` (required): Excel file (.xlsx, .xls, .xlsm)
  - `sheets` (optional): Comma-separated sheet names

**Response:**
```json
{
  "status": "success",
  "file": "contracts.xlsx",
  "timestamp": "2026-03-18T10:45:30.654321",
  "dry_run": true,
  "steps": {
    "business_units": {
      "status": "completed",
      "message": "Completed business_units"
    },
    "clients": {
      "status": "completed",
      "message": "Completed clients"
    }
  }
}
```

**HTTP Status Codes:**
- `200` - Validation successful
- `400` - Bad request or validation failed
- `500` - Server error

---

## cURL Examples

### 1. Health Check
```bash
curl http://localhost:5000
```

### 2. Import Full File
```bash
curl -X POST \
  -F "file=@path/to/contracts.xlsx" \
  http://localhost:5000/import
```

### 3. Dry Run (Validate)
```bash
curl -X POST \
  -F "file=@path/to/contracts.xlsx" \
  http://localhost:5000/import/dry-run
```

### 4. Import Specific Sheets
```bash
curl -X POST \
  -F "file=@path/to/contracts.xlsx" \
  -F "sheets=clients,agreements,sows" \
  http://localhost:5000/import
```

---

## JavaScript Fetch Examples

### 1. Health Check
```javascript
fetch('http://localhost:5000')
  .then(r => r.json())
  .then(data => console.log(data));
```

### 2. Import File
```javascript
const file = document.querySelector('input[type="file"]').files[0];
const formData = new FormData();
formData.append('file', file);

fetch('http://localhost:5000/import', {
  method: 'POST',
  body: formData
})
.then(r => r.json())
.then(data => console.log(data));
```

### 3. Dry Run
```javascript
const file = document.querySelector('input[type="file"]').files[0];
const formData = new FormData();
formData.append('file', file);

fetch('http://localhost:5000/import/dry-run', {
  method: 'POST',
  body: formData
})
.then(r => r.json())
.then(data => console.log(data));
```

### 4. Import with Specific Sheets
```javascript
const file = document.querySelector('input[type="file"]').files[0];
const formData = new FormData();
formData.append('file', file);
formData.append('sheets', 'clients,agreements,sows');

fetch('http://localhost:5000/import', {
  method: 'POST',
  body: formData
})
.then(r => r.json())
.then(data => console.log(data));
```

---

## Axios Examples

### 1. Import File
```javascript
const file = document.querySelector('input[type="file"]').files[0];
const formData = new FormData();
formData.append('file', file);

axios.post('http://localhost:5000/import', formData)
  .then(r => console.log(r.data))
  .catch(e => console.error(e));
```

### 2. Dry Run
```javascript
axios.post('http://localhost:5000/import/dry-run', formData)
  .then(r => console.log(r.data))
  .catch(e => console.error(e));
```

---

## Error Responses

### 400 - Missing File
```json
{
  "status": "error",
  "message": "No file provided. Please upload an Excel file."
}
```

### 400 - Invalid File Format
```json
{
  "status": "error",
  "message": "Invalid file type. Allowed: xlsx, xls, xlsm"
}
```

### 413 - File Too Large
```json
{
  "status": "error",
  "message": "File too large. Maximum size: 50.0MB"
}
```

### 500 - Server Error
```json
{
  "status": "error",
  "message": "Internal server error",
  "error": "..."
}
```

---

## Import Steps

Các bước import theo thứ tự:

1. `business_units` - Seed business units
2. `system_user` - Seed ETL system user
3. `clients` - Import clients
4. `agreements` - Import agreements
5. `agreement_insurance` - Import agreement insurance
6. `sows` - Import statements of work
7. `partnerships` - Import partnerships

---

## Constraints

- **Max file size**: 50MB
- **Allowed formats**: .xlsx, .xls, .xlsm
- **File upload timeout**: Depends on file size
- **Database connection**: Required
- **CORS**: Enabled (localhost origin)

---

## Environment Variables

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=db_contract
DB_USER=postgres
DB_PASSWORD=123
FLASK_DEBUG=True
PORT=5000
```

---

## Status Codes Reference

| Code | Meaning |
|------|---------|
| 200 | OK - Request successful |
| 400 | Bad Request - Invalid input |
| 413 | Payload Too Large - File too big |
| 500 | Internal Server Error |

---

## Response Status Values

- `success` - Health check or dry-run successful
- `imported` - File imported to database successfully
- `error` - Operation failed

---
