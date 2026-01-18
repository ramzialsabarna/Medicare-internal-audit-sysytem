# results_pipeline/result_audit_notes_parser_pandas.py
# ============================================================
# AUDIT NOTES PARSER (RESULTS)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Breaks down the unstructured 'VISIT_RESULT_NOTES' column.
# - Uses Keyword-based splitting: (ملاحظات, ملاحظات تطويرية, بنود عدم المطابقة).
# - Produces 3 separate cleaned columns to avoid mixed-intent data.
# - This prepares qualitative text for later sentiment or NLP analysis.
# ============================================================

from __future__ import annotations
import pandas as pd
import re
from results_pipeline.core_utilities_results_pandas import normalize_text, ensure_column_df

def process_visit_notes_splitting(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parses the main notes column into structured sub-categories.
    """
    target_col = "VISIT_RESULT_NOTES"
    if target_col not in df.columns:
        return df

    df = df.copy()
    
    # الأعمدة الجديدة المستهدفة
    new_cols = ["NOTES_GENERAL", "NOTES_DEVELOPMENTAL", "NOTES_NC_EVIDENCE"]
    for c in new_cols:
        ensure_column_df(df, c)

    def _parse_notes(val):
        if pd.isna(val) or str(val).strip() == "":
            return None, None, None
        
        text = str(val)
        
        # منطق التقسيم: نبحث عن الكلمات المفتاحية ونأخذ النص الذي يليها
        # ملاحظة: هذا التعبير النمطي يبحث عن الأجزاء بناءً على العناوين التي ذكرتها
        general = re.search(r"ملاحظات\s*:(.*?)(?=ملاحظات تطويرية|بنود عدم المطابقة|$)", text, re.S)
        dev = re.search(r"ملاحظات تطويرية\s*:(.*?)(?=بنود عدم المطابقة|$)", text, re.S)
        nc_ev = re.search(r"بنود عدم المطابقة\s*:(.*)", text, re.S)
        
        return (
            general.group(1).strip() if general else None,
            dev.group(1).strip() if dev else None,
            nc_ev.group(1).strip() if nc_ev else None
        )

    # تطبيق التفكيك
    parsed_data = df[target_col].apply(_parse_notes)
    df["NOTES_GENERAL"] = [x[0] for x in parsed_data]
    df["NOTES_DEVELOPMENTAL"] = [x[1] for x in parsed_data]
    df["NOTES_NC_EVIDENCE"] = [x[2] for x in parsed_data]

    return df