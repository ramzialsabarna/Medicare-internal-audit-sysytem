# results_pipeline/result_audit_plan_mapping_pandas.py — Updated for Mass Balance Mapping
# ==============================================================================
# AUDIT PLAN MAPPING LOGIC:
# This script uses Regex patterns to categorize audit plan texts into codes.
# NEW ADDITION: Mass Balance Calibration Renewal (Ramallah Central Lab).
# ==============================================================================

from __future__ import annotations
import pandas as pd
import re

AUDIT_PLAN_RULES = [
    # 1. NEW: Mass Balance Calibration Renewal (تجديد معايرة ميزان الكتلة)
    # This rule looks for (Mass/Balance/كتلة/ميزان) + (Renewal/Certificate/تجديد/شهادة)
    ("mass_balance_calibration_renewal", [
        r"(?=.*(لميزان|كتلة|كتله|mass|balance))(?=.*(تجديد|شهادة|شهاده|renewal|المعايرة|معايره))"
    ]),

    # 2. القاعدة الأكثر تحديداً: موازين حرارية (يجب أن يحتوي النص على موازين/حرارة + تجديد/شهادة)
    ("standard_thermometer_certificate_renewal", [
        r"(?=.*(حراري|thermometer))(?=.*(تجديد|شهادة|شهاده|renewal|معايرة|معايره))"
    ]),
    
    # 3. قاعدة الأيزو: (يجب أن يحتوي على ايزو + تجديد)
    ("iso_certification_renewal", [
        r"(?=.*(ايزو|الأيزو|iso))(?=.*(تجديد|renewal|certification))"
    ]),

    # 4. باقی القواعد الاعتيادية
    ("quarter_1", [r"\bq\s*1\b", r"\bquarter\s*1\b", r"\bq1\b", r"(?:الربع|ربع).*(?:الأول|الاول|اول)"]),
    ("quarter_2", [r"\bq\s*2\b", r"\bquarter\s*2\b", r"\bq2\b", r"(?:الربع|ربع).*(?:الثاني|ثاني)"]),
    ("quarter_3", [r"\bq\s*3\b", r"\bquarter\s*3\b", r"\bq3\b", r"(?:الربع|ربع).*(?:الثالث|ثالث)"]),
    ("quarter_4", [r"\bq\s*4\b", r"\bquarter\s*4\b", r"\bq4\b", r"(?:الربع|ربع).*(?:الرابع|رابع)"]),
    
    ("iso15189_clause_compliance_audit", [
        r"15189", r"\biso\s*15189\b", r"ايزو\s*15189", r"الأيزو\s*15189",
        r"مواصف(?:ه|ة)\s*15189", r"المواصف(?:ه|ة)\s*15189", r"\bclause\b", r"بنود"
    ]),
    
    ("lis_formula_verification_audit", [
        r"\blis\b", r"\bformula\b", r"\bcalculation\b", r"معادل",
        r"عمليات\s*حساب", r"العمليات\s*الحساب", r"\bsilim\b", r"sليم",
        r"verification", r"تحقق"
    ]),
    
    ("note_followup", [
        r"\bnote\s*follow\s*-?\s*up\b", r"\bfollow\s*-?\s*up\b", r"\bnote\b",
        r"متابع(?:ة|ه)", r"\bfollowup\b"
    ]),
]

def map_plan_by_rules(text: str) -> str | None:
    if not text or pd.isna(text):
        return None
    
    text_str = str(text).lower()
    for code, patterns in AUDIT_PLAN_RULES:
        for pattern in patterns:
            # استخدام IGNORECASE لضمان التقاط الكلمات بكل حالاتها
            if re.search(pattern, text_str, re.IGNORECASE | re.DOTALL):
                return code
    return "unmapped_plan"

def process_result_audit_plan_mapping(df: pd.DataFrame) -> pd.DataFrame:
    if "AUDIT_PLAN" not in df.columns:
        return df
    df["AUDIT_PLAN_CODE"] = df["AUDIT_PLAN"].apply(map_plan_by_rules)
    return df