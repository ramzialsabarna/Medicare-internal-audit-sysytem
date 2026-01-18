# data_schema_inspector.py — Inspect Excel Metadata and Data Types
# ============================================================
# LOGIC & METHODOLOGY:
# - Scans the latest Excel workbook for Structure and Result sheets.
# - Analyzes the technical schema of each sheet (Column Names, Data Types).
# - Detects data quality metrics: Unique value counts and Null (missing) values.
# - Helps identify "Mixed Type" columns that could cause processing errors.
# ============================================================

from __future__ import annotations
import pandas as pd
from pathlib import Path

# --- Configuration ---
FILE_PREFIX = "ISO_DATA"

def find_latest_file(prefix: str) -> Path:
    """Finds the most recent Excel file in the directory starting with prefix."""
    files = list(Path(".").glob(f"{prefix}*.xlsx"))
    if not files:
        raise FileNotFoundError(f"No file found starting with: {prefix}")
    return sorted(files, reverse=True)[0]

def inspect_data_types(xlsx_path: Path) -> None:
    """Reads Excel sheets and prints a technical summary of column schemas."""
    xls = pd.ExcelFile(xlsx_path)
    
    print(f"\n" + "="*80)
    print(f"🧐 SCHEMA INSPECTOR: Technical Analysis for {xlsx_path.name}")
    print("="*80)

    for sheet in xls.sheet_names:
        # Only inspect relevant Structure and Result sheets
        if not (sheet.startswith("ISO_Check_") or sheet.startswith("visit_result")):
            continue
            
        print(f"\n📄 SHEET: [{sheet}]")
        print("-" * 40)
        
        # Load the sheet data
        df = pd.read_excel(xlsx_path, sheet_name=sheet)
        
        # Gather metadata for each column
        schema_info = []
        for col in df.columns:
            dtype = df[col].dtype
            
            # Count unique values and missing entries
            unique_count = df[col].nunique()
            null_count = df[col].isna().sum()
            
            # Clarify data type (Python labels strings as 'object')
            actual_type = "Text/String" if dtype == 'object' else str(dtype)
            
            schema_info.append({
                "Column Name": col,
                "Data Type": actual_type,
                "Unique Values": unique_count,
                "Missing Values": null_count
            })
        
        # Display the results in a formatted table
        schema_df = pd.DataFrame(schema_info)
        print(schema_df.to_string(index=False))
        print("-" * 80)

if __name__ == "__main__":
    try:
        target_file = find_latest_file(FILE_PREFIX)
        inspect_data_types(target_file)
        
        print("\n" + "="*80)
        print("✅ SCHEMA ANALYSIS COMPLETE: All relevant sheets inspected.")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ INSPECTOR ERROR: {str(e)}")