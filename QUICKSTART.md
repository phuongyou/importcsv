# Quick Start Guide - CTA ETL Import Server

## 5-Minute Setup

### Step 1: Install Dependencies

```batch
cd g:\2025_10_27_CTA_Project\tool\importcsv

# Create virtual environment
python -m venv venv

# Activate it
venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

### Step 2: Configure Database

Set environment variables (PowerShell):

```powershell
$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_NAME="db_contract"
$env:DB_USER="postgres"
$env:DB_PASSWORD="123"
$env:FLASK_DEBUG="True"
$env:PORT="5000"
```

Or create `.env` file:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=db_contract
DB_USER=postgres
DB_PASSWORD=123
FLASK_DEBUG=True
PORT=5000
```

Copy from `.env.example`:
```batch
copy .env.example .env
# Edit .env with your database credentials
```

### Step 3: Start Server

```batch
# Ensure virtual environment is activated
venv\Scripts\activate

# Run the server
python app.py
```

You should see:
```
Starting CTA ETL Import Server on port 5000
Upload folder: g:\2025_10_27_CTA_Project\tool\importcsv\uploads
 * Running on http://0.0.0.0:5000
```

### Step 4: Test the API

In a new terminal:

```batch
# Activate venv
venv\Scripts\activate

# Run tests
python test_server.py
```

Or manually test with curl:

```batch
curl http://localhost:5000
```

You should see JSON response with health status.

---

## Using from React

### 1. Add Component

Copy `ETLImporter.jsx` to your React project:

```bash
src/components/ETLImporter.jsx
```

### 2. Import and Use

```jsx
import ETLImporter from './components/ETLImporter';

function App() {
  return <ETLImporter />;
}
```

### 3. Configure URL (if needed)

Create `.env` in React project:

```env
REACT_APP_ETL_SERVER=http://localhost:5000
```

### 4. Test Upload

1. Open your React app
2. Use ETLImporter component to upload Excel file
3. View import results

---

## Manual Testing with curl

### Health Check
```batch
curl http://localhost:5000
```

### Dry Run (Validate)
```batch
curl -X POST ^
  -F "file=@path\to\your\file.xlsx" ^
  http://localhost:5000/import/dry-run
```

### Full Import
```batch
curl -X POST ^
  -F "file=@path\to\your\file.xlsx" ^
  http://localhost:5000/import
```

### Import Specific Sheets
```batch
curl -X POST ^
  -F "file=@path\to\your\file.xlsx" ^
  -F "sheets=clients,agreements,sows" ^
  http://localhost:5000/import
```

---

## Troubleshooting

### Issue: Server won't start

**Problem:** `ModuleNotFoundError: No module named 'flask'`

**Fix:**
```batch
venv\Scripts\activate
pip install -r requirements.txt
```

---

### Issue: Database connection error

**Problem:** `psycopg2.OperationalError: could not connect to server`

**Fix:**
1. Check PostgreSQL is running
2. Verify credentials in `.env`
3. Test connection:
   ```batch
   psql -h localhost -U postgres -d db_contract
   ```

---

### Issue: CORS error in React

**Fix:** This is already handled by Flask-CORS in `app.py`

Check that:
1. Server is running on correct port (5000)
2. React .env has `REACT_APP_ETL_SERVER=http://localhost:5000`
3. Frontend and backend URLs match

---

### Issue: File upload fails

**Check:**
1. File is `.xlsx`, `.xls`, or `.xlsm` format
2. File size < 50MB
3. `uploads/` folder exists and is writable
4. Check server logs for detailed error

---

## File Structure

```
importcsv/
├── app.py                           # Flask server
├── etl_import.py                    # ETL processing logic
├── ETLImporter.jsx                  # React component
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment template
├── test_server.py                   # API tests
├── SERVER_README.md                 # Full documentation
├── QUICKSTART.md                    # This file
├── ETL_README.md                    # Original ETL docs
├── uploads/                         # Uploaded files (auto-created)
└── venv/                           # Virtual environment (auto-created)
```

---

## Next Steps

1. ✅ Server running and tested
2. ✅ React component integrated
3. Try uploading a test Excel file
4. Monitor database for imported records
5. Check `uploads/` folder for file references

---

## Common Commands Reference

| Task | Command |
|------|---------|
| Activate venv | `venv\Scripts\activate` |
| Install deps | `pip install -r requirements.txt` |
| Start server | `python app.py` |
| Test API | `python test_server.py` |
| Check DB | `psql -d db_contract -c "\dt"` |
| View logs | Open server output |
| Stop server | `Ctrl+C` |

---

## Additional Resources

- **Full Documentation:** [SERVER_README.md](SERVER_README.md)
- **Original ETL Guide:** [ETL_README.md](ETL_README.md)
- **Flask Docs:** https://flask.palletsprojects.com/
- **PostgreSQL Docs:** https://www.postgresql.org/docs/

---

## Support

**Server not starting?**
- Check activation: `venv\Scripts\activate`
- Check dependencies: `pip list | findstr flask`
- Check Python: `python --version`

**File upload failing?**
- Check file format (must be .xlsx/.xls/.xlsm)
- Check file size (max 50MB)
- Run test_server.py for detailed diagnostics

**Database errors?**
- Verify DB is running: `psql --version`
- Check credentials in .env
- Check schema exists: `psql -d db_contract -c "\dt"`

---

## Done! 🎉

Your ETL server is ready to use with React. Upload files through the React interface or directly via API.
