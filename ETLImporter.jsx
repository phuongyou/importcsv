// ─── React Component for ETL Import ───────────────────────────
// Save this as: src/components/ETLImporter.jsx
// or in your React project

import React, { useState } from "react";
import axios from "axios";

const ETL_SERVER_URL = process.env.REACT_APP_ETL_SERVER || "http://localhost:5000";

export default function ETLImporter() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [dryRun, setDryRun] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [selectedSheets, setSelectedSheets] = useState("");

  const handleFileChange = (e) => {
    const selectedFile = e.target.files?.[0];
    if (selectedFile) {
      setFile(selectedFile);
      setError(null);
      setResult(null);
    }
  };

  const handleImport = async () => {
    if (!file) {
      setError("Please select a file first");
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    const formData = new FormData();
    formData.append("file", file);
    if (selectedSheets) {
      formData.append("sheets", selectedSheets);
    }

    try {
      const endpoint = dryRun ? "/import/dry-run" : "/import";
      const response = await axios.post(`${ETL_SERVER_URL}${endpoint}`, formData, {
        headers: {
          "Content-Type": "multipart/form-data",
        },
      });

      setResult(response.data);
      if (response.data.status === "error") {
        setError(response.data.message || "Import failed");
      }
    } catch (err) {
      const message =
        err.response?.data?.message ||
        err.message ||
        "Failed to import file";
      setError(message);
      setResult(err.response?.data || null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={styles.container}>
      <h1>CTA ETL Import</h1>

      <div style={styles.section}>
        <h2>Upload Excel File</h2>
        <div style={styles.form}>
          <input
            type="file"
            accept=".xlsx,.xls,.xlsm"
            onChange={handleFileChange}
            disabled={loading}
            style={styles.input}
          />

          {file && (
            <p style={styles.fileName}>
              Selected: <strong>{file.name}</strong> ({(file.size / 1024).toFixed(2)} KB)
            </p>
          )}

          <div style={styles.options}>
            <label style={styles.label}>
              <input
                type="checkbox"
                checked={dryRun}
                onChange={(e) => setDryRun(e.target.checked)}
                disabled={loading}
              />
              {" "} Dry Run (parse only, no import)
            </label>

            <div style={styles.sheetsInput}>
              <label>
                Sheets to import (comma-separated, optional):
                <input
                  type="text"
                  value={selectedSheets}
                  onChange={(e) => setSelectedSheets(e.target.value)}
                  placeholder="e.g., clients, agreements, sows"
                  disabled={loading}
                  style={styles.input}
                />
              </label>
            </div>
          </div>

          <button
            onClick={handleImport}
            disabled={!file || loading}
            style={{
              ...styles.button,
              opacity: !file || loading ? 0.6 : 1,
            }}
          >
            {loading ? "Importing..." : "Import to Database"}
          </button>
        </div>
      </div>

      {error && (
        <div style={styles.error}>
          <h3>Error</h3>
          <p>{error}</p>
        </div>
      )}

      {result && (
        <div style={styles.result}>
          <h2>✅ Import Results</h2>
          <div style={styles.resultContent}>
            <p>
              <strong>Status:</strong> {result.status}
            </p>
            <p>
              <strong>File:</strong> {result.file}
            </p>
            <p>
              <strong>Time:</strong> {new Date(result.timestamp).toLocaleString()}
            </p>

            {result.dry_run && (
              <p style={{ color: "blue" }}>
                <strong>⚠️ This was a dry run - no data was imported</strong>
              </p>
            )}

            {result.summary && Object.keys(result.summary).length > 0 && (
              <div style={styles.summaryBox}>
                <h3>📊 Summary - Records Inserted by Sheet:</h3>
                <div style={styles.summaryGrid}>
                  {Object.entries(result.summary).map(([sheetName, count]) => (
                    <div key={sheetName} style={styles.summaryItem}>
                      <span style={styles.summaryLabel}>
                        {sheetName.charAt(0).toUpperCase() + sheetName.slice(1)}:
                      </span>
                      <span style={styles.summaryCount}>{count}</span>
                      <span style={styles.summaryText}> records</span>
                    </div>
                  ))}
                </div>
                {result.total_records > 0 && (
                  <div style={styles.summaryTotal}>
                    <strong>Total Records Inserted: {result.total_records}</strong>
                  </div>
                )}
              </div>
            )}

            {result.steps && (
              <div style={styles.steps}>
                <h3>📋 Detailed Steps:</h3>
                {Object.entries(result.steps).map(([stepName, stepResult]) => (
                  <div
                    key={stepName}
                    style={{
                      ...styles.step,
                      borderLeft:
                        (stepResult.status === "completed" ? "3px solid green" : "3px solid red"),
                    }}
                  >
                    <strong>{stepName}</strong>:{" "}
                    <span
                      style={{
                        color:
                          stepResult.status === "completed" ? "green" : "red",
                      }}
                    >
                      {stepResult.status}
                    </span>
                    <p>{stepResult.message}</p>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      <div style={styles.info}>
        <h3>Info</h3>
        <ul>
          <li>Server: {ETL_SERVER_URL}</li>
          <li>Supported formats: .xlsx, .xls, .xlsm</li>
          <li>Max file size: 50MB</li>
          <li>Use Dry Run to validate without importing</li>
        </ul>
      </div>
    </div>
  );
}

const styles = {
  container: {
    maxWidth: "800px",
    margin: "0 auto",
    padding: "20px",
    fontFamily: "Arial, sans-serif",
  },
  section: {
    backgroundColor: "#f5f5f5",
    padding: "20px",
    borderRadius: "8px",
    marginBottom: "20px",
  },
  form: {
    display: "flex",
    flexDirection: "column",
    gap: "15px",
  },
  input: {
    padding: "8px",
    border: "1px solid #ddd",
    borderRadius: "4px",
    fontSize: "14px",
  },
  fileName: {
    color: "#666",
    margin: "0",
  },
  options: {
    display: "flex",
    flexDirection: "column",
    gap: "10px",
  },
  label: {
    display: "flex",
    alignItems: "center",
    cursor: "pointer",
  },
  sheetsInput: {
    display: "flex",
    flexDirection: "column",
    gap: "5px",
  },
  button: {
    padding: "10px 20px",
    backgroundColor: "#007bff",
    color: "white",
    border: "none",
    borderRadius: "4px",
    cursor: "pointer",
    fontSize: "16px",
  },
  error: {
    backgroundColor: "#f8d7da",
    color: "#721c24",
    padding: "15px",
    borderRadius: "4px",
    marginBottom: "20px",
    border: "1px solid #f5c6cb",
  },
  result: {
    backgroundColor: "#d4edda",
    padding: "20px",
    borderRadius: "4px",
    marginBottom: "20px",
    border: "1px solid #c3e6cb",
  },
  resultContent: {
    backgroundColor: "#fff",
    padding: "15px",
    borderRadius: "4px",
  },
  summaryBox: {
    backgroundColor: "#fff3cd",
    padding: "15px",
    borderRadius: "4px",
    marginBottom: "20px",
    border: "1px solid #ffc107",
    marginTop: "15px",
  },
  summaryGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
    gap: "10px",
    marginTop: "10px",
    marginBottom: "15px",
  },
  summaryItem: {
    backgroundColor: "#f0f0f0",
    padding: "10px",
    borderRadius: "4px",
    display: "flex",
    alignItems: "center",
    gap: "8px",
    borderLeft: "4px solid #28a745",
  },
  summaryLabel: {
    fontWeight: "bold",
    color: "#333",
  },
  summaryCount: {
    fontSize: "18px",
    fontWeight: "bold",
    color: "#28a745",
  },
  summaryText: {
    color: "#666",
    fontSize: "12px",
  },
  summaryTotal: {
    backgroundColor: "#28a745",
    color: "white",
    padding: "12px",
    borderRadius: "4px",
    textAlign: "center",
    fontSize: "16px",
  },
  steps: {
    marginTop: "15px",
  },
  step: {
    padding: "10px",
    marginBottom: "10px",
    backgroundColor: "#f9f9f9",
    borderRadius: "3px",
  },
  info: {
    backgroundColor: "#e7f3ff",
    padding: "15px",
    borderRadius: "4px",
    border: "1px solid #b3d9ff",
  },
};
