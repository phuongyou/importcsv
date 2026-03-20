# CTA ETL Import Server Setup Guide

## Overview

This document explains how to set up and run the ETL Import Server that allows your React application to upload and process Excel files for database import.

---

## Architecture

```
React Frontend (ETLImporter.jsx)
         ↓ (HTTP POST with file)
   [Flask Server (app.py)]
         ↓
   [ETL Processing (etl_import.py)]
         ↓
   PostgreSQL Database
```

### API Endpoints

- **`POST /import`** - Upload and import Excel file to database
- **`POST /import/dry-run`** - Validate Excel file without importing
- **`GET /`** - Health check and API documentation

---

## Prerequisites

- Python 3.8+
- PostgreSQL server running and configured
- Node.js/React for the frontend (optional, server works standalone)

---

## Installation

### 1. Install Python Dependencies

```bash
cd g:\2025_10_27_CTA_Project\tool\importcsv

# Create virtual environment (recommended)
python -m venv venv

# Activate virtual environment
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Database

#### Option A: Using Environment Variables

```bash
# Windows PowerShell
$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_NAME="db_contract"
$env:DB_USER="postgres"
$env:DB_PASSWORD="123"
```

#### Option B: Using .env File

Create a `.env` file in the same directory as `app.py`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=db_contract
DB_USER=postgres
DB_PASSWORD=123
FLASK_DEBUG=True
PORT=5000
```

### 3. Prepare Database

Ensure your PostgreSQL database and schema are created:

```bash
# Create database (if not exists)
psql -U postgres -c "CREATE DATABASE db_contract;"

# Run schema script
psql -U postgres -d db_contract -f CTA_PostgreSQL_Schema.sql
```

---

## Running the Server

### Start the Flask Server

```bash
# Activate virtual environment first
venv\Scripts\activate

# Run the server
python app.py
```

Expected output:
```
Starting CTA ETL Import Server on port 5000
Upload folder: g:\2025_10_27_CTA_Project\tool\importcsv\uploads
 * Running on http://0.0.0.0:5000
```

### Verify Server is Running

Open in browser:
```
http://localhost:5000
```

You should see a JSON response with health status and available endpoints.

---

## Running with Docker

### Option 1: Using Docker Compose (Recommended)

```bash
# Build and start the container
docker-compose up --build

# Run in background
docker-compose up -d --build
```

Expected output:
```
cta-etl-server | Starting CTA ETL Import Server on port 5000
cta-etl-server |  * Running on http://0.0.0.0:5000
```

### Option 2: Using Docker Directly

```bash
# Build the Docker image
docker build -t cta-etl-server .

# Run the container
docker run -p 5000:5000 \
  -e DB_HOST=localhost \
  -e DB_PORT=5432 \
  -e DB_NAME=db_contract \
  -e DB_USER=postgres \
  -e DB_PASSWORD=123 \
  -v ./uploads:/app/uploads \
  cta-etl-server
```

### Verify Docker Container is Running

```bash
# Check running containers
docker ps

# View logs
docker logs cta-etl-server

# Test the API
curl http://localhost:5000
```

### Troubleshooting Docker

**Error: "Could not locate a Flask application"**
- Solution: The Dockerfile already sets `FLASK_APP=app.py`, so this error shouldn't occur. If it does, verify the Dockerfile is in the correct directory.

**Database Connection Issues**
- If using a local PostgreSQL (not in Docker), change `DB_HOST=localhost` to `DB_HOST=host.docker.internal` (Windows/Mac) or `172.17.0.1` (Linux)
- Alternatively, uncomment the PostgreSQL service in `docker-compose.yml` to run both in Docker

**Port Already in Use**
- Change the port mapping: `docker run -p 8000:5000 ...` (access via `http://localhost:8000`)

### Stop Docker Container

```bash
# Stop specific container
docker stop cta-etl-server

# Or if using docker-compose
docker-compose down
```

---

## API Usage

### 1. Upload and Import File

**Endpoint:** `POST /import`

**Request (using curl):**
```bash
curl -X POST \
  -F "file=@Viscosity_Master_Contracts_File_-_CTA.xlsx" \
  http://localhost:5000/import
```

**Request (using Python):**
```python
import requests

with open("Viscosity_Master_Contracts_File_-_CTA.xlsx", "rb") as f:
    files = {"file": f}
    response = requests.post("http://localhost:5000/import", files=files)
    print(response.json())
```

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

### 2. Dry Run (Validate Only)

**Endpoint:** `POST /import/dry-run`

Validates the file without importing:

```bash
curl -X POST \
  -F "file=@Viscosity_Master_Contracts_File_-_CTA.xlsx" \
  http://localhost:5000/import/dry-run
```

### 3. Import Specific Sheets Only

Add sheets parameter (comma-separated):

```bash
curl -X POST \
  -F "file=@Viscosity_Master_Contracts_File_-_CTA.xlsx" \
  -F "sheets=clients,agreements,sows" \
  http://localhost:5000/import
```

### 4. Health Check

```bash
curl http://localhost:5000
```

---

## React Frontend Integration

### 1. Install axios (if needed)

```bash
npm install axios
```

### 2. Add Component to Your App

Copy `ETLImporter.jsx` to your React components folder:

```bash
src/components/ETLImporter.jsx
```

### 3. Use in Your App

```jsx
import ETLImporter from './components/ETLImporter';

function App() {
  return (
    <div>
      <ETLImporter />
    </div>
  );
}

export default App;
```

### 4. Configure Server URL (Optional)

Create a `.env` file in your React project:

```env
REACT_APP_ETL_SERVER=http://localhost:5000
```

Or set it in production:

```env
REACT_APP_ETL_SERVER=https://your-production-server.com:5000
```

---

## Docker Setup (Optional)

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV FLASK_DEBUG=False
ENV PORT=5000

EXPOSE 5000

CMD ["python", "app.py"]
```

Build and run:

```bash
docker build -t cta-etl-server .
docker run -p 5000:5000 \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_NAME=db_contract \
  -e DB_USER=postgres \
  -e DB_PASSWORD=123 \
  cta-etl-server
```

---

## Troubleshooting

### Issue: "No module named 'psycopg2'"

```bash
pip install psycopg2-binary
```

### Issue: "Connection refused" (DB error)

Check database connection:
```bash
psql -h localhost -p 5432 -U postgres -d db_contract
```

### Issue: "CORS error from React"

The Flask server already has CORS enabled. If you still get CORS errors:

1. Check server is running on the correct URL
2. Update `REACT_APP_ETL_SERVER` in React `.env`
3. Ensure frontend and backend URLs match

### Issue: File upload fails

1. Check file size (max 50MB)
2. Verify file format (.xlsx, .xls, .xlsm only)
3. Check server logs for detailed error messages
4. Ensure `uploads/` folder exists and is writable

### Issue: Database import fails

1. Check PostgreSQL is running and configured
2. Verify schema exists: `psql -d db_contract -c "\dt"`
3. Check environment variables are set correctly
4. Review server logs for detailed error message

---

## Production Deployment

### 1. Use Production WSGI Server

Instead of Flask's development server, use Gunicorn:

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

### 2. Enable HTTPS

Use Nginx as reverse proxy with SSL:

```nginx
server {
    listen 443 ssl;
    server_name your-domain.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### 3. Environment Variables

Use a secrets manager instead of .env:

```bash
# Set in production environment
export DB_PASSWORD="secure-password-here"
export FLASK_ENV="production"
export FLASK_DEBUG="False"
```

### 4. Logging

Configure proper logging:

```python
# In app.py
import logging
from logging.handlers import RotatingFileHandler

file_handler = RotatingFileHandler('etl_server.log', maxBytes=10000000, backupCount=10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
app.logger.addHandler(file_handler)
```

---

## File Upload Handling

Uploaded files are stored in the `uploads/` folder with timestamps to prevent overwrites:

```
uploads/
├── 20260318_093012_Viscosity_Master_Contracts_File_-_CTA.xlsx
├── 20260318_095630_Viscosity_Master_Contracts_File_-_CTA.xlsx
└── dryrun_20260318_104512_test_file.xlsx
```

You can configure cleanup in `app.py` to automatically delete files after successful import.

---

## API Response Examples

### Successful Import
```json
{
  "status": "imported",
  "file": "contracts.xlsx",
  "timestamp": "2026-03-18T10:30:45.123456",
  "dry_run": false,
  "steps": { ... }
}
```

### Error Response
```json
{
  "status": "error",
  "message": "Invalid Excel file format",
  "traceback": "..."
}
```

### Validation Error
```json
{
  "status": "error",
  "message": "No file provided. Please upload an Excel file."
}
```

---

## Support

For issues or questions:
1. Check server logs: `etl_server.log`
2. Enable debug mode: `FLASK_DEBUG=True`
3. Test with small file first
4. Verify database connection using psql

---

## Version History

- **v1.0.0** (2026-03-18) - Initial Flask server implementation
  - File upload support
  - Dry-run validation
  - Partial sheet import
  - CORS enabled for React

---
