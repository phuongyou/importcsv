#!/usr/bin/env python3
"""
Flask Server: ETL Import API
Converts etl_import.py into a REST API for React to upload Excel files
"""

import os
import sys
import logging
import json
import tempfile
from datetime import datetime
from typing import Dict, Any, Tuple
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
import traceback

# Import ETL functions
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from etl_import import (
    seed_business_units,
    import_clients,
    import_agreements,
    import_sows,
    import_partnerships,
    get_conn,
    log as etl_log
)

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)  # Enable CORS for React

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("server")

# File upload configuration
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), "uploads")
ALLOWED_EXTENSIONS = {"xlsx", "xls", "xlsm"}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_SIZE


# ─── HELPER FUNCTIONS ────────────────────────────────────────────────────────

def allowed_file(filename: str) -> bool:
    """Check if file has allowed extension"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def process_excel_file(file_path: str, dry_run: bool = False, sheets: list = None) -> Dict[str, Any]:
    """
    Process an Excel file and import to database
    
    Args:
        file_path: Path to the Excel file
        dry_run: If True, parse only without writing to DB
        sheets: List of sheets to import (None = all)
    
    Returns:
        Dict with import results and status
    """
    
    # Validate file exists and is readable
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    import pandas as pd
    
    # Verify it's a valid Excel file
    try:
        pd.read_excel(file_path, sheet_name=0, nrows=1)
    except Exception as e:
        raise ValueError(f"Invalid or corrupted Excel file: {str(e)}")
    
    results = {
        "status": "success",
        "file": os.path.basename(file_path),
        "timestamp": datetime.now().isoformat(),
        "dry_run": dry_run,
        "steps": {}
    }
    
    try:
        # Override EXCEL_PATH in etl_import module
        import etl_import
        etl_import.EXCEL_PATH = file_path
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Step 1: Seed business units (always required)
        try:
            log.info(f"Running step: business_units")
            count = seed_business_units(cur)
            if not dry_run:
                conn.commit()
            results["steps"]["business_units"] = {
                "status": "completed",
                "message": f"Completed business_units - {count} records"
            }
            log.info(f"✓ business_units completed - {count} records")
        except Exception as e:
            log.error(f"✗ business_units failed: {str(e)}")
            results["steps"]["business_units"] = {
                "status": "error",
                "message": str(e)
            }
            conn.rollback()
            raise
        
        # Step 2: Import clients (needed for agreements and sows)
        client_map = {}
        try:
            log.info(f"Running step: clients")
            client_map, count = import_clients(cur)
            if not dry_run:
                conn.commit()
            results["steps"]["clients"] = {
                "status": "completed",
                "message": f"Completed clients - {count} records"
            }
            log.info(f"✓ clients completed - {count} records")
        except Exception as e:
            log.error(f"✗ clients failed: {str(e)}")
            results["steps"]["clients"] = {
                "status": "error",
                "message": str(e)
            }
            conn.rollback()
        
        # Step 4: Import agreements
        try:
            log.info(f"Running step: agreements")
            # Backup: if client_map is empty, reload from DB
            if not client_map:
                log.info("  Loading clients from DB...")
                cur.execute("SELECT company_name, client_id FROM clients")
                client_map = {r[0]: r[1] for r in cur.fetchall()}
            
            agr_map, count = import_agreements(cur, client_map)
            if not dry_run:
                conn.commit()
            results["steps"]["agreements"] = {
                "status": "completed",
                "message": f"Completed agreements - {count} records"
            }
            log.info(f"✓ agreements completed - {count} records")
        except Exception as e:
            log.error(f"✗ agreements failed: {str(e)}")
            results["steps"]["agreements"] = {
                "status": "error",
                "message": str(e)
            }
            conn.rollback()
        
        # Step 4: Import sows
        try:
            log.info(f"Running step: sows")
            # Backup: if client_map is empty, reload from DB
            if not client_map:
                log.info("  Loading clients from DB...")
                cur.execute("SELECT company_name, client_id FROM clients")
                client_map = {r[0]: r[1] for r in cur.fetchall()}
            
            count = import_sows(cur, client_map)
            if not dry_run:
                conn.commit()
            results["steps"]["sows"] = {
                "status": "completed",
                "message": f"Completed sows - {count} records"
            }
            log.info(f"✓ sows completed - {count} records")
        except Exception as e:
            log.error(f"✗ sows failed: {str(e)}")
            results["steps"]["sows"] = {
                "status": "error",
                "message": str(e)
            }
            conn.rollback()
        
        # Step 5: Import partnerships
        try:
            log.info(f"Running step: partnerships")
            count = import_partnerships(cur)
            if not dry_run:
                conn.commit()
            results["steps"]["partnerships"] = {
                "status": "completed",
                "message": f"Completed partnerships - {count} records"
            }
            log.info(f"✓ partnerships completed - {count} records")
        except Exception as e:
            log.error(f"✗ partnerships failed: {str(e)}")
            results["steps"]["partnerships"] = {
                "status": "error",
                "message": str(e)
            }
            conn.rollback()
        
        cur.close()
        conn.close()
        
        log.info("All steps completed successfully")
        results["status"] = "success" if dry_run else "imported"
        
        # Create a summary with record counts
        summary = {}
        total_records = 0
        for step_name, step_info in results["steps"].items():
            if step_info["status"] == "completed":
                # Extract record count from message
                message = step_info.get("message", "")
                # Expected format: "Completed {step_name} - {count} records"
                import re
                match = re.search(r'(\d+)\s+records?', message)
                if match:
                    count = int(match.group(1))
                    summary[step_name] = count
                    total_records += count
        
        results["summary"] = summary
        results["total_records"] = total_records
        
    except Exception as e:
        log.error(f"Import failed: {str(e)}\n{traceback.format_exc()}")
        results["status"] = "error"
        results["error"] = str(e)
        results["traceback"] = traceback.format_exc()
    
    return results


# ─── API ENDPOINTS ───────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "service": "CTA ETL Import Server",
        "version": "1.0.0",
        "endpoints": {
            "POST /import": "Upload and import Excel file",
            "POST /import/dry-run": "Parse Excel file without importing",
            "GET /health": "Health check",
        }
    })


@app.route("/import", methods=["POST"])
def import_file():
    """
    Upload and import Excel file to database
    
    Request:
        - file: Excel file (multipart/form-data)
        - sheets: Optional comma-separated sheet names to import
    
    Response:
        - status: "success" or "error"
        - steps: Results of each import step
    """
    
    # Check if file is present
    if "file" not in request.files:
        return jsonify({
            "status": "error",
            "message": "No file provided. Please upload an Excel file."
        }), 400
    
    file = request.files["file"]
    
    # Check if file has a name
    if file.filename == "":
        return jsonify({
            "status": "error",
            "message": "No file selected"
        }), 400
    
    # Validate file extension
    if not allowed_file(file.filename):
        return jsonify({
            "status": "error",
            "message": f"Invalid file type. Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
        }), 400
    
    try:
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_")
        temp_filename = timestamp + filename
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], temp_filename)
        
        file.save(file_path)
        log.info(f"File uploaded: {file_path}")
        
        # Parse sheets parameter if provided
        sheets = None
        if "sheets" in request.form:
            sheets = [s.strip() for s in request.form.get("sheets", "").split(",")]
            sheets = [s for s in sheets if s]
        
        # Process the file
        result = process_excel_file(file_path, dry_run=False, sheets=sheets)
        
        # Keep the file for reference (optional - set to delete after import)
        # os.remove(file_path)
        
        status_code = 200 if result["status"] == "imported" else 400
        return jsonify(result), status_code
        
    except Exception as e:
        log.error(f"Import error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/import/dry-run", methods=["POST"])
def import_file_dry_run():
    """
    Parse Excel file without importing to database (validation only)
    
    Request:
        - file: Excel file (multipart/form-data)
        - sheets: Optional comma-separated sheet names to check
    
    Response:
        - status: "success" or "error"
        - dry_run: true
        - steps: Results of validation
    """
    
    # Check if file is present
    if "file" not in request.files:
        return jsonify({
            "status": "error",
            "message": "No file provided. Please upload an Excel file."
        }), 400
    
    file = request.files["file"]
    
    # Check if file has a name
    if file.filename == "":
        return jsonify({
            "status": "error",
            "message": "No file selected"
        }), 400
    
    # Validate file extension
    if not allowed_file(file.filename):
        return jsonify({
            "status": "error",
            "message": f"Invalid file type. Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
        }), 400
    
    try:
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_filename = f"dryrun_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], temp_filename)
        
        file.save(file_path)
        log.info(f"Dry-run file: {file_path}")
        
        # Parse sheets parameter if provided
        sheets = None
        if "sheets" in request.form:
            sheets = [s.strip() for s in request.form.get("sheets", "").split(",")]
            sheets = [s for s in sheets if s]
        
        # Process the file (dry run mode)
        result = process_excel_file(file_path, dry_run=True, sheets=sheets)
        
        # Clean up temp file
        os.remove(file_path)
        
        return jsonify(result), 200
        
    except Exception as e:
        log.error(f"Dry-run error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.errorhandler(413)
def request_entity_too_large(error):
    """Handle file too large error"""
    return jsonify({
        "status": "error",
        "message": f"File too large. Maximum size: {MAX_FILE_SIZE / 1024 / 1024}MB"
    }), 413


@app.errorhandler(500)
def internal_error(error):
    """Handle internal server error"""
    return jsonify({
        "status": "error",
        "message": "Internal server error",
        "error": str(error)
    }), 500


# ─── MAIN ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", True)
    
    log.info(f"Starting CTA ETL Import Server on port {port}")
    log.info(f"Upload folder: {UPLOAD_FOLDER}")
    
    # Suppress Werkzeug development server warning
    import logging
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    
    app.run(host="0.0.0.0", port=port, debug=debug, use_reloader=False)
