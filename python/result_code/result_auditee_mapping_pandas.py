# results_pipeline/result_auditee_mapping_pandas.py
# ============================================================
# AUDITEE TO USER-MASTER MAPPING (BILINGUAL & MULTI-LINE)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Supports both Arabic (FULL_NAME) and English (FULL_NAME_EN) from 'users' sheet.
# - Processes multi-line entries (names separated by newlines).
# - Logic: One ID can be reached via multiple name aliases (Ar/En).
# - Ensures no matter which language the auditor uses, the ID remains consistent.
# ============================================================

from __future__ import annotations
import pandas as pd
from results_pipeline.core_utilities_results_pandas import normalize_text, is_missing_like

def map_multi_auditees_bilingual(results_df: pd.DataFrame, users_df: pd.DataFrame) -> pd.DataFrame:
    """
    Maps auditee names using BOTH Arabic and English columns from the internal users sheet.
    """
    if users_df is None or users_df.empty:
        print("⚠️ WARNING: Users Master DataFrame is empty. Skipping mapping.")
        return results_df

    # بناء "قاموس العناوين الذكي" - يدعم لغتين لكل ID
    user_map = {}
    
    for _, row in users_df.iterrows():
        off_id = str(row.get('ID', ''))
        ar_name = str(row.get('FULL_NAME', ''))
        en_name = str(row.get('FULL_NAME_EN', ''))
        
        # ربط الاسم العربي بالـ ID
        if not is_missing_like(ar_name):
            norm_ar = normalize_text(ar_name).replace(" ", "")
            user_map[norm_ar] = (ar_name, off_id)
            
        # ربط الاسم الإنجليزي بنفس الـ ID
        if not is_missing_like(en_name):
            norm_en = normalize_text(en_name).replace(" ", "")
            user_map[norm_en] = (en_name, off_id)

    results_df = results_df.copy()

    def _process_cell(raw_cell):
        """تفكيك الخلية والبحث في القاموس الثنائي اللغة."""
        if is_missing_like(raw_cell):
            return None, None
            
        parts = [p.strip() for p in str(raw_cell).split('\n') if p.strip()]
        found_names, found_ids = [], []
        
        for p in parts:
            clean_p = normalize_text(p).replace(" ", "")
            matched_name, matched_id = f"Unknown({p})", "N/A"
            
            # البحث عن تطابق (سواء كان المدقق كتب بالعربي أو الإنجليزي)
            if clean_p in user_map:
                matched_name, matched_id = user_map[clean_p]
            else:
                # بحث جزئي ذكي في كل المفاتيح (عربي وإنجليزي)
                for norm_key, (orig_name, u_id) in user_map.items():
                    if clean_p in norm_key or norm_key in clean_p:
                        matched_name, matched_id = orig_name, u_id
                        break
            
            found_names.append(matched_name)
            found_ids.append(matched_id)
            
        return "; ".join(found_names), "; ".join(found_ids)

    # تطبيق الربط
    if "AUDITEE" in results_df.columns:
        mapped_data = results_df['AUDITEE'].apply(_process_cell)
        results_df['AUDITEE_OFFICIAL_NAME'] = [x[0] for x in mapped_data]
        results_df['AUDITEE_USER_ID'] = [x[1] for x in mapped_data]

    return results_df