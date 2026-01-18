# ============================================================
# Master Sequential Runner: Unique UVL Logic & Data Integrity
# ============================================================

from __future__ import annotations
from pathlib import Path
import pandas as pd

# --- 1. استيراد الموديولات الأساسية والهوية ---
from results_pipeline.result_bulk_identifier_tokens_pandas import process_result_identifier_tokens as token_func
from results_pipeline.result_bulk_numeric_metrics_pandas import process_result_numeric_metrics as num_func
from results_pipeline.result_bulk_datetime_processing_pandas import process_result_bulk_datetime as date_func
from results_pipeline.result_category_pandas import process_result_category_columns as cat_func
from results_pipeline.result_branch_columns import process_result_branch_columns as branch_func
from results_pipeline.result_audit_parties_pandas import process_result_parties_mapping as parties_func

# --- 2. استيراد موديولات البنود والإجابات (بناء ITEM_KEY) ---
from results_pipeline.result_item_columns_pandas import process_result_item_columns as item_func
from results_pipeline.result_answer_columns_pandas import process_result_answer_columns as ans_func

# --- 3. استيراد موديولات حالات النجاح/الفشل الفريدة (UVL Unique Status) ---
from results_pipeline.result_item_score_status_pandas import process_result_item_score_status_columns as item_status_func
from results_pipeline.result_category_score_status_pandas import process_result_category_score_status_columns as cat_status_func

# --- 4. استيراد موديولات الملاحظات وحالات عدم المطابقة (NC) ---
from results_pipeline.result_audit_notes_parser_pandas import process_visit_notes_splitting as notes_func
from results_pipeline.result_nc_tracking_pandas import process_result_nc_tracking as nc_func

# --- 5. استيراد موديولات التصنيف والمطابقة النهائية ---
from results_pipeline.result_item_classification_columns_pandas import process_result_item_classification_columns as class_func
from results_pipeline.result_visit_status_columns import process_result_visit_status_columns as visit_status_func
from results_pipeline.result_visit_total_status_pandas import process_result_visit_result_score_status_columns as visit_total_score_func
from results_pipeline.result_audit_plan_mapping_pandas import process_result_audit_plan_mapping as map_func
from results_pipeline.results_matcher import build_structure_reference_from_workbook, match_results_df_to_structure

# Configuration
from config.domain_config import (
    CATEGORY_SPELLING_MAP, 
    VISIT_STATUS_MAP, 
    VISIT_RESULT_STATUS_MAP, 
    BRANCH_NAME_OVERRIDES,
    DEP_LABELS
)

# =========================
# CONFIGURATION CONSTANTS
# =========================
FILE_PREFIX = "ISO_DATA" 
STRUCT_PREFIX = "ISO_Check_"
RESULT_PREFIX = "visit_result"
REPORT_PREFIX = "rep_"

def prepare_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {'CATEGORY_ID': 'CHECK_CATEGORY_ID', 'ITEM_ID': 'CHECK_ITEM_ID', 'ANSWER_ID': 'CHECK_ANSWER_ID'}
    for old, new in mapping.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]
    return df

def run_master_pipeline(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    df = prepare_raw_columns(df)
    
    # المرحلة 1: التطهير الأساسي (الأرقام، التواريخ، التوكنات)
    print("      .. Step 1: Base Data Sanitization")
    df = num_func(df)   
    df = date_func(df)  
    df = token_func(df) # تحويل الـ IDs لنصوص نظيفة
    
    # المرحلة 2: الهوية والأطراف
    print("      .. Step 2: Identity (Category, Branch, Parties)")
    df = cat_func(df=df, sheet_name=sheet_name, category_spelling_map=CATEGORY_SPELLING_MAP)
    df = branch_func(df=df, branch_overrides=BRANCH_NAME_OVERRIDES, dep_labels=DEP_LABELS)
    df = parties_func(df) 
    
    # المرحلة 3: بناء المفاتيح الفريدة (Critical Step for UVL)
    print("      .. Step 3: Building Item Keys & Features")
    df = item_func(df) # ينشئ ITEM_KEY الأساسي للفرادة
    df = ans_func(df)  # ينشئ أكواد الإجابات الفريدة المرتبطة بالبند
    
    # المرحلة 4: ترميز الحالات والنتائج بنظام الفرادة (UVL Encoding)
    print("      .. Step 4: Unique Status Encoding (coss_ & iss_)")
    # حالة البند: iss_cat__item_pass
    df = item_status_func(df, visit_result_status_map=VISIT_RESULT_STATUS_MAP)
    # حالة الكاتيجوري: coss_cat_pass
    df = cat_status_func(df, visit_result_status_map=VISIT_RESULT_STATUS_MAP)
    
    # المرحلة 5: معالجة الملاحظات وتتبع NC
    print("      .. Step 5: Notes & NC Tracking (Unique Logic)")
    df = notes_func(df) # تفكيك الملاحظات لـ 3 أعمدة
    df = nc_func(df)    # إنشاء أكواد NC الفريدة تحت الـ ITEM_KEY
    
    # المرحلة 6: التصنيف والربط مع الهيكل
    print("      .. Step 6: Final Classification & Integrity Checks")
    df = class_func(df)
    df = visit_status_func(df, visit_status_map=VISIT_STATUS_MAP)
    df = visit_total_score_func(df, visit_result_status_map=VISIT_RESULT_STATUS_MAP)
    df = map_func(df)
    
    return df

def main() -> None:
    print("\n" + "="*60)
    print("🚀 Master Runner: UVL Unique Structure Mode")
    print("="*60)
    
    try:
        # البحث عن الملف
        xlsx_path = sorted(list(Path(".").glob(f"{FILE_PREFIX}*.xlsx")), reverse=True)[0]
        xls = pd.ExcelFile(xlsx_path)
        
        all_struct_sheets = [s for s in xls.sheet_names if s.startswith(STRUCT_PREFIX)]
        all_result_sheets = [s for s in xls.sheet_names if s.startswith(RESULT_PREFIX)]

        print(f"📂 Active Workbook: {xlsx_path.name}")
        ref = build_structure_reference_from_workbook(str(xlsx_path), all_struct_sheets)

        final_sheets = {}
        # نسخ الشيتات التي لا تبدأ بـ Result كما هي
        for s in xls.sheet_names:
            if not s.startswith(RESULT_PREFIX):
                final_sheets[s] = pd.read_excel(xlsx_path, sheet_name=s)

        # معالجة شيتات النتائج
        for sheet in all_result_sheets:
            print(f"\n[PHASE] Processing -> {sheet}")
            df_raw = pd.read_excel(xlsx_path, sheet_name=sheet)
            df_processed = run_master_pipeline(df_raw, sheet_name=sheet)
            
            print(f"      .. Matching with Structure Reference")
            matching_results = match_results_df_to_structure(
                df_results=df_processed, full_ref=ref, result_sheet_name=sheet
            )
            
            final_sheets[sheet] = df_processed
            final_sheets[f"{REPORT_PREFIX}{sheet}_summary"] = matching_results["summary"]
            
        # حفظ النتائج في الملف الأصلي (In-place)
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            for sheet_name, content_df in final_sheets.items():
                content_df.to_excel(writer, sheet_name=sheet_name, index=False)

        print("\n🎉 SUCCESS: All Unique UVL Features generated and mapped!")

    except Exception as e:
        print(f"\n❌ RUNTIME ERROR: {str(e)}")

if __name__ == "__main__":
    main()