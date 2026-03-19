#!/usr/bin/env python3
"""
Test script for CTA ETL Import Server
Tests all API endpoints and provides debugging information
"""

import os
import sys
import requests
import json
from pathlib import Path
import time

# Configuration
SERVER_URL = os.getenv("ETL_SERVER_URL", "http://localhost:5000")
EXCEL_FILE = None

def find_excel_file():
    """Find first Excel file in current directory"""
    current_dir = Path(__file__).parent
    excel_exts = ('.xlsx', '.xls', '.xlsm')
    for file in current_dir.iterdir():
        if file.suffix.lower() in excel_exts:
            return file
    return None


def test_health_check():
    """Test health check endpoint"""
    print("\n" + "="*60)
    print("TEST 1: Health Check")
    print("="*60)
    
    try:
        response = requests.get(f"{SERVER_URL}/")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False


def test_dry_run(file_path):
    """Test dry-run endpoint"""
    print("\n" + "="*60)
    print("TEST 2: Dry-Run (Validation Only)")
    print("="*60)
    
    if not file_path or not file_path.exists():
        print(f"ERROR: File not found: {file_path}")
        return False
    
    print(f"Testing with file: {file_path.name}")
    
    try:
        with open(file_path, "rb") as f:
            files = {"file": f}
            response = requests.post(f"{SERVER_URL}/import/dry-run", files=files)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        return response.status_code == 200
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False


def test_import(file_path, sheets=None):
    """Test import endpoint"""
    print("\n" + "="*60)
    print("TEST 3: Full Import")
    print("="*60)
    
    if not file_path or not file_path.exists():
        print(f"ERROR: File not found: {file_path}")
        return False
    
    print(f"Testing with file: {file_path.name}")
    if sheets:
        print(f"Sheets: {sheets}")
    
    try:
        with open(file_path, "rb") as f:
            files = {"file": f}
            data = {}
            if sheets:
                data["sheets"] = sheets
            
            response = requests.post(
                f"{SERVER_URL}/import",
                files=files,
                data=data
            )
        
        print(f"Status Code: {response.status_code}")
        result = response.json()
        
        # Pretty print the response
        print(f"Status: {result.get('status')}")
        print(f"File: {result.get('file')}")
        print(f"Timestamp: {result.get('timestamp')}")
        
        if result.get('steps'):
            print("\nImport Steps:")
            for step_name, step_result in result['steps'].items():
                status = step_result.get('status')
                message = step_result.get('message')
                print(f"  {step_name}: {status} - {message}")
        
        if result.get('error'):
            print(f"\nError: {result['error']}")
        
        return response.status_code in (200, 400)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False


def test_import_specific_sheets(file_path):
    """Test importing specific sheets only"""
    print("\n" + "="*60)
    print("TEST 4: Import Specific Sheets")
    print("="*60)
    
    if not file_path or not file_path.exists():
        print(f"ERROR: File not found: {file_path}")
        return False
    
    print(f"Testing with file: {file_path.name}")
    print("Sheets: clients,agreements")
    
    try:
        with open(file_path, "rb") as f:
            files = {"file": f}
            data = {"sheets": "clients,agreements"}
            
            response = requests.post(
                f"{SERVER_URL}/import",
                files=files,
                data=data
            )
        
        print(f"Status Code: {response.status_code}")
        result = response.json()
        print(f"Response: {json.dumps(result, indent=2)}")
        
        return response.status_code in (200, 400)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False


def test_invalid_file():
    """Test with invalid file"""
    print("\n" + "="*60)
    print("TEST 5: Invalid File (Error Handling)")
    print("="*60)
    
    try:
        # Create a temporary invalid file
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp.write(b"This is not an Excel file")
            tmp_path = tmp.name
        
        with open(tmp_path, "rb") as f:
            files = {"file": f}
            response = requests.post(f"{SERVER_URL}/import", files=files)
        
        os.unlink(tmp_path)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        # Should return 400 for invalid file
        return response.status_code == 400
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False


def test_no_file():
    """Test without file"""
    print("\n" + "="*60)
    print("TEST 6: No File (Error Handling)")
    print("="*60)
    
    try:
        response = requests.post(f"{SERVER_URL}/import")
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        # Should return 400 for missing file
        return response.status_code == 400
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("CTA ETL Import Server - API Tests")
    print("="*60)
    print(f"Server URL: {SERVER_URL}")
    
    # Find Excel file
    EXCEL_FILE = find_excel_file()
    if EXCEL_FILE:
        print(f"Test Excel file: {EXCEL_FILE.name}")
    else:
        print("⚠️ WARNING: No Excel file found in current directory")
        print("   Tests 2-4 will be skipped")
    
    # Run tests
    results = {}
    
    results["Health Check"] = test_health_check()
    time.sleep(1)
    
    if EXCEL_FILE:
        results["Dry-Run"] = test_dry_run(EXCEL_FILE)
        time.sleep(1)
        
        # ASK user before full import
        print("\n" + "!"*60)
        user_input = input("RUN FULL IMPORT TEST? (This will modify the database) [y/N]: ").strip().lower()
        
        if user_input == "y":
            results["Full Import"] = test_import(EXCEL_FILE)
            time.sleep(1)
            
            results["Import Specific Sheets"] = test_import_specific_sheets(EXCEL_FILE)
            time.sleep(1)
        else:
            results["Full Import"] = None
            results["Import Specific Sheets"] = None
    
    results["Invalid File"] = test_invalid_file()
    time.sleep(1)
    
    results["No File"] = test_no_file()
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for v in results.values() if v is True)
    failed = sum(1 for v in results.values() if v is False)
    skipped = sum(1 for v in results.values() if v is None)
    
    for test_name, result in results.items():
        if result is True:
            status = "✓ PASSED"
        elif result is False:
            status = "✗ FAILED"
        else:
            status = "⊘ SKIPPED"
        print(f"{test_name:.<40} {status}")
    
    print("="*60)
    print(f"Total: {passed} passed, {failed} failed, {skipped} skipped")
    print("="*60)
    
    if failed == 0:
        print("✓ All tests passed!")
        return 0
    else:
        print(f"✗ {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
