# results_pipeline/results_matcher.py
# ============================================================
# RESULTS Matcher (Pandas) — Full Integration with Score Status
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Matches Results with Structure based on sheet numeric suffixes.
# - Transfers VISIT_RESULT_SCORE_STATUS_CODE from Structure to Results.
# - Handles "Planned" audit type logic and "Missing Item" exceptions.
# ============================================================

from __future__ import annotations
from typing import Dict, List, Set, Any
import pandas as pd
import re

# استيراد الأدوات المساعدة
from results_pipeline.core_utilities_results_pandas import is_missing_like

# --- CONFIG: أعمدة الستركشر ---
STRUCT_ITEM_FEATURE_COL    = "ITEM_FEATURE_NAME"
STRUCT_ANS_FEATURE_COL     = "ANSWER_FEATURE_NAME"
STRUCT_PLAN_CODE_COL       = "AUDIT_PLAN_CODE"
# العمود الجديد في الستركشر
STRUCT_SCORE_STATUS_COL    = "VISIT_RESULT_SCORE_STATUS_CODE"

# --- CONFIG: أعمدة النتائج ---
RES_ITEM_FEATURE_COL       = "ITEM_FEATURE_NAME"
RES_ANS_FEATURE_COL        = "ANSWER_FEATURE_NAME"
RES_PLAN_CODE_COL          = "AUDIT_PLAN_CODE"
RES_AUDIT_TYPE_CODE_COL    = "AUDIT_TYPE_CODE"


def build_structure_reference_from_workbook(xlsx_path: str, structure_sheets: List[str]) -> Dict[str, Dict[str, Any]]:
    """يبني مرجعاً من الستركشر مع الاحتفاظ بنتيجة التقييم والبنود والإجابات."""
    reference = {}
    for sh in structure_sheets:
        df = pd.read_excel(xlsx_path, sheet_name=sh)
        
        # استخراج البنود مع حالاتها (Score Status)
        items_map = {}
        if STRUCT_ITEM_FEATURE_COL in df.columns:
            # نأخذ البند والنتيجة المقابلة له
            sub_df = df[[STRUCT_ITEM_FEATURE_COL, STRUCT_SCORE_STATUS_COL]].dropna(subset=[STRUCT_ITEM_FEATURE_COL])
            for _, row in sub_df.iterrows():
                items_map[row[STRUCT_ITEM_FEATURE_COL]] = row[STRUCT_SCORE_STATUS_COL]
        
        answers = set(df[STRUCT_ANS_FEATURE_COL].dropna().unique()) if STRUCT_ANS_FEATURE_COL in df.columns else set()
        plans = set(df[STRUCT_PLAN_CODE_COL].dropna().unique()) if STRUCT_PLAN_CODE_COL in df.columns else set()
        
        reference[sh] = {
            "items_map": items_map,  # قاموس بدلاً من set لحفظ الـ Score Status
            "answers": answers, 
            "plans": plans
        }
    return reference


def match_results_df_to_structure(df_results: pd.DataFrame, full_ref: Dict[str, Any], result_sheet_name: str) -> Dict[str, pd.DataFrame]:
    """يطابق النتائج بالستركشر وينقل عمود Score Status لضمان سلامة الـ UVL."""
    
    # 1. تحديد شيت الستركشر المستهدف بناءً على الرقم
    sheet_num_list = re.findall(r'\d+', result_sheet_name)
    if not sheet_num_list: return {}
    sheet_num = sheet_num_list[0]
    target_struct_key = next((k for k in full_ref.keys() if k.endswith(sheet_num)), None)
    if not target_struct_key: return {}

    ref = full_ref[target_struct_key]
    df = df_results.copy()
    
    # تحضير أعمدة التتبع
    df["_item_ok"] = False
    df["_ans_ok"] = False
    df["STRUCT_SCORE_STATUS"] = None # العمود الذي سيتم نقله من الستركشر

    # 2. منطق مطابقة البنود والإجابات + نقل الـ Score Status
    for idx, row in df.iterrows():
        item_val = row.get(RES_ITEM_FEATURE_COL)
        ans_val = row.get(RES_ANS_FEATURE_COL)
        
        # معالجة استثناء البند الفارغ (زيارة تذكيرية)
        if is_missing_like(item_val):
            df.at[idx, "_item_ok"] = True
            df.at[idx, "_ans_ok"] = True
            continue
            
        # المطابقة الفعلية للبند
        if item_val in ref["items_map"]:
            df.at[idx, "_item_ok"] = True
            # نقل الـ Score Status من المرجع إلى صف النتيجة
            df.at[idx, "STRUCT_SCORE_STATUS"] = ref["items_map"][item_val]
            
        # المطابقة الفعلية للإجابة
        if is_missing_like(ans_val) or ans_val in ref["answers"]:
            df.at[idx, "_ans_ok"] = True

    # 3. منطق مطابقة الخطة الشرطية (Planned Only)
    def check_plan_logic(row):
        audit_type_code = str(row.get(RES_AUDIT_TYPE_CODE_COL, "")).lower().strip()
        plan_code = row.get(RES_PLAN_CODE_COL)
        if audit_type_code == "planned":
            return plan_code in ref["plans"]
        return True

    df["_plan_ok"] = df.apply(check_plan_logic, axis=1)

    # 4. تجهيز التقارير
    available_opt = [c for c in ["VISIT_ID", "VISIT_DATE", "CHECK_ITEM_NAME", "BRANCH_NAME", RES_AUDIT_TYPE_CODE_COL] if c in df.columns]
    
    unmatched_items = df[~df["_item_ok"]][[RES_ITEM_FEATURE_COL] + available_opt].copy()
    unmatched_answers = df[~df["_ans_ok"]][[RES_ANS_FEATURE_COL] + available_opt].copy()
    unmatched_plans = df[~df["_plan_ok"]][[RES_PLAN_CODE_COL] + available_opt].copy()

    # ملخص النتائج النهائي
    summary = pd.DataFrame([{
        "result_sheet": result_sheet_name,
        "matched_with_struct": target_struct_key,
        "total_records": len(df),
        "items_ok": df["_item_ok"].sum(),
        "answers_ok": df["_ans_ok"].sum(),
        "plans_ok": df["_plan_ok"].sum(),
        "status": "PASS" if (df["_item_ok"].all() and df["_plan_ok"].all()) else "FAIL"
    }])

    return {
        "processed_df": df, # أعدنا الـ DataFrame المعالج ليستخدمه الرنر
        "summary": summary,
        "unmatched_items": unmatched_items,
        "unmatched_answers": unmatched_answers,
        "unmatched_plans": unmatched_plans
    }