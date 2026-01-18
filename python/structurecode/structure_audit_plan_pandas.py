# structure_audit_plan_pandas.py
# -------------------------------------------------------------------
# UNIFIED | AUDIT_PLAN Processing Module (INTELLIGENT + UVL REPRESENTATION)
# -------------------------------------------------------------------

from __future__ import annotations

import re
import pandas as pd

from core_utilities_structure_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
)

_COL_RAW = "AUDIT_PLAN"
_COL_CLEAN = "AUDIT_PLAN_CLEAN"
_COL_CODE = "AUDIT_PLAN_CODE"

# =============================================================================
# RESULTS-LIKE MAPPING RULES (Regex) - FINAL OPTIMIZED
# =============================================================================
AUDIT_PLAN_RULES: list[tuple[str, list[str]]] = [
    
    # 1) ISO 15189 Specific
    ("iso15189_clause_compliance_audit", [
        r"(?=.*15189)(?=.*(ايزو|iso|مواصف|بنود|تدقيق|اعتماد|compliance|clause))"
    ]),

    # 2) ISO General Certification Renewal
    ("iso_certification_renewal", [
        r"(?=.*(ايزو|الأيزو|iso))(?=.*(تجديد|renewal|certification|شهادة|شهاده))"
    ]),

    # 3) LIS Formula Verification
    ("lis_formula_verification_audit", [
        r"(?=.*(حساب|معادل|lis|silim))(?=.*(عمليات|تحقق|متابعة|متابعه|verification|formula))"
    ]),

    # 4) Note Follow-up
    ("note_followup", [
        r"(?=.*(متابعة|متابعه|follow))(?=.*(ملاحظات|notes|followup))"
    ]),

    # 5) Calibration & Renewal
    ("mass_balance_calibration_renewal", [
        r"(?=.*(ميزان|كتلة|balance))(?=.*(تجديد|معايرة|شهادة|renewal))"
    ]),
    ("standard_thermometer_certificate_renewal", [
        r"(?=.*(حراري|ترمومتر|thermometer))(?=.*(تجديد|معايرة|شهادة|renewal))"
    ]),

    # 6) Quarters
    ("quarter_1", [r"(?:الربع|للربع).*(?:الأول|الاول|اول|q1|q\s*1)"]),
    ("quarter_2", [r"(?:الربع|للربع).*(?:الثاني|ثاني|q2|q\s*2)"]),
    ("quarter_3", [r"(?:الربع|للربع).*(?:الثالث|ثالث|q3|q\s*3)"]),
    ("quarter_4", [r"(?:الربع|للربع).*(?:الرابع|رابع|q4|q\s*4)"]),
]

def _map_plan_by_rules(text: str | None) -> str | None:
    """
    يحاول التصنيف الذكي أولاً، وإذا فشل يعود للتمثيل الحرفي (Fallback to to_uvl_code)
    """
    if text is None or is_missing_like(text):
        return None

    text_norm = normalize_text(text)

    # المرحلة 1: التصنيف الذكي (النتائج المتوقعة للأرباع والمواصفات)
    for code, patterns in AUDIT_PLAN_RULES:
        for pattern in patterns:
            if re.search(pattern, text_norm, flags=re.IGNORECASE | re.DOTALL):
                return code

    # المرحلة 2: التمثيل الحرفي (UVL Representation) للحالات غير المعرفة
    # هذا يضمن أنك لن تفقد "التمثيل" لأي جملة جديدة أو غريبة
    return to_uvl_code(text_norm)


def process_audit_plan(df: pd.DataFrame, sheet_name: str | None = None, mode: str = "structure") -> pd.DataFrame:
    """
    المحرك الموحد النهائي:
    - ينظف النص في AUDIT_PLAN_CLEAN.
    - يطبق التصنيف الذكي مع Fallback في AUDIT_PLAN_CODE.
    - يحافظ على القيم الموجودة مسبقاً ولا يمسحها.
    """
    if _COL_RAW not in df.columns:
        return df

    df = df.copy()
    ensure_column_df(df, _COL_CLEAN)
    ensure_column_df(df, _COL_CODE)

    # تنظيف النص
    df[_COL_CLEAN] = df[_COL_RAW].apply(
        lambda x: normalize_text(x).strip() if not is_missing_like(x) else None
    )

    # وظيفة حساب الكود الذكي
    def _compute_code(row) -> str | None:
        existing = row.get(_COL_CODE, None)
        # إذا كان الكود موجوداً مسبقاً (غير فارغ)، لا نغيره
        if not is_missing_like(existing):
            return existing 
        
        # إذا كان فارغاً، نطبق محرك القواعد + تمثيل UVL
        return _map_plan_by_rules(row.get(_COL_CLEAN, None))

    df[_COL_CODE] = df.apply(_compute_code, axis=1)
    
    return df

# للإبقاء على التوافق مع الاستدعاءات القديمة (Aliasing)
process_structure_audit_plan_df = process_audit_plan
process_result_audit_plan_mapping = process_audit_plan