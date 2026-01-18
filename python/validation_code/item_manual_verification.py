import pandas as pd
import os

"""
ITEM VALIDATION SAMPLING (Updated Column Map):
- Matches the specific columns found: ITEM_FEATURE_NAME, ITEM_KEY, ITEM_TEXT_CODE, ITEM_NAME_CLEAN.
- Maintains CHECK_ITEM_NAME as the primary original reference.
- Consolidates samples from all relevant sheets for manual audit.
"""

# ---------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------
FILE_PREFIX = "ISO_DATA"
RESULTS_PREFIX = "visit_result"
STRUCT_PREFIX = "ISO_Check_category"
OUTPUT_SHEET = "ITEM_MANUAL_SAMPLE"
SAMPLE_PER_SHEET = 10 
RANDOM_STATE = 42

files = [f for f in os.listdir('.') if f.startswith(FILE_PREFIX) and f.endswith('.xlsx')]

if not files:
    print(f"❌ Error: File starting with '{FILE_PREFIX}' not found.")
else:
    FILE_PATH = files[0]
    print(f"📂 Working File: {FILE_PATH}")
    
    excel_file = pd.ExcelFile(FILE_PATH)
    all_target_sheets = [s for s in excel_file.sheet_names 
                         if s.lower().startswith(RESULTS_PREFIX.lower()) 
                         or s.lower().startswith(STRUCT_PREFIX.lower())]

    if not all_target_sheets:
        print(f"❌ Error: No matching sheets found.")
    else:
        all_samples = []
        
        # Updated mapping based on your extracted columns
        col_map = {
            "ORIGINAL_ITEM": "CHECK_ITEM_NAME",
            "CLEAN_NAME": "ITEM_NAME_CLEAN",
            "FEATURE_NAME": "ITEM_FEATURE_NAME",
            "ITEM_KEY": "ITEM_KEY",
            "TEXT_CODE": "ITEM_TEXT_CODE"
        }

        for sheet_name in all_target_sheets:
            print(f"🔍 Processing sheet: {sheet_name}...", end=" ")
            try:
                df = pd.read_excel(FILE_PATH, sheet_name=sheet_name)
                # Standardize headers to uppercase for matching
                df.columns = [c.upper() for c in df.columns]

                # Verify if the original column exists
                if col_map["ORIGINAL_ITEM"] not in df.columns:
                    print(f"⚠️ Skipped (Original column '{col_map['ORIGINAL_ITEM']}' not found).")
                    continue

                # Drop empty rows in the original item name column
                clean_df = df.dropna(subset=[col_map["ORIGINAL_ITEM"]])
                if clean_df.empty:
                    print(f"⚠️ Skipped (Sheet is empty).")
                    continue

                # Take random sample
                actual_size = min(SAMPLE_PER_SHEET, len(clean_df))
                sheet_sample = clean_df.sample(n=actual_size, random_state=RANDOM_STATE)

                # Prepare the subset based on your specific columns
                sample_subset = pd.DataFrame()
                for label, actual_col in col_map.items():
                    if actual_col in sheet_sample.columns:
                        sample_subset[label] = sheet_sample[actual_col]
                    else:
                        sample_subset[label] = "N/A" # Fill if a specific column is missing in one version

                # Metadata for traceability
                sample_subset["SOURCE_SHEET"] = sheet_name
                all_samples.append(sample_subset)
                print(f"✅ Sampled {len(sample_subset)} rows.")

            except Exception as e:
                print(f"❌ Error: {e}")

        # ---------------------------------------------------------
        # 2. Consolidation & Export
        # ---------------------------------------------------------
        if all_samples:
            final_df = pd.concat(all_samples, ignore_index=True, sort=False)
            
            # Add manual verification columns
            final_df["MATCH_STATUS"] = "" # You will fill this: (Correct / Incorrect)
            final_df["NOTES"] = ""

            # Reorder for the best visual comparison flow
            final_cols = ["SOURCE_SHEET", "ITEM_KEY", "TEXT_CODE", "ORIGINAL_ITEM", 
                          "CLEAN_NAME", "FEATURE_NAME", "MATCH_STATUS", "NOTES"]
            
            final_df = final_df[[c for c in final_cols if c in final_df.columns]]

            with pd.ExcelWriter(FILE_PATH, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                final_df.to_excel(writer, sheet_name=OUTPUT_SHEET, index=False)

            print("\n" + "="*50)
            print(f"🚀 SUCCESS: Item Validation Sheet '{OUTPUT_SHEET}' is ready.")
            print(f"Total rows to review: {len(final_df)}")
            print("="*50)