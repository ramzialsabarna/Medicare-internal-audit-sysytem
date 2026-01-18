# result_branch_columns.py
# ============================================================
# Branch / Audited Entity Encoding (RESULTS) — Mirror Pipeline
# ------------------------------------------------------------
# What this module does:
# - Replicates the exact Branch classification and ID tokenization for Results.
# - Ensures that Results recorded at a specific branch match the Structure definition.
# ============================================================

from __future__ import annotations
from typing import Dict, Set, Optional
import pandas as pd

from core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
    normalize_id_token,
)

def process_result_branch_columns(
    df: pd.DataFrame,
    branch_overrides: Dict[str, str],
    dep_labels: Set[str],
) -> pd.DataFrame:
    if "BRANCH_ID" not in df.columns or "BRANCH_NAME" not in df.columns:
        return df

    df = df.copy()

    ensure_column_df(df, "BRANCH_LABEL_CLEAN")
    ensure_column_df(df, "ENTITY_TYPE")
    ensure_column_df(df, "BRANCH_LABEL_SLUG")
    ensure_column_df(df, "BRANCH_ID_TOKEN")
    ensure_column_df(df, "BRANCH_FEATURE_CODE")

    dep_slugs = {to_uvl_code(x) for x in dep_labels if x}
    dep_slugs.discard(None)

    def _clean_and_override(raw_name) -> Optional[str]:
        if is_missing_like(raw_name): return None
        name = normalize_text(raw_name).strip()
        return branch_overrides.get(name, name) if name else None

    df["BRANCH_LABEL_CLEAN"] = df["BRANCH_NAME"].apply(_clean_and_override)
    df["BRANCH_LABEL_SLUG"] = df["BRANCH_LABEL_CLEAN"].apply(to_uvl_code)

    def _classify(label_clean: Optional[str]) -> Optional[str]:
        if not label_clean: return None
        slug = to_uvl_code(label_clean)
        if not slug: return None
        if slug in dep_slugs: return "dep"
        if slug == "headoffice" or slug.startswith("elite_medical"): return "org"
        return "branch"

    df["ENTITY_TYPE"] = df["BRANCH_LABEL_CLEAN"].apply(_classify)

    def _id_token(raw_id) -> Optional[str]:
        if is_missing_like(raw_id): return None
        tok = normalize_id_token(raw_id)
        return tok if tok else None

    df["BRANCH_ID_TOKEN"] = df["BRANCH_ID"].apply(_id_token)

    def _make_feature(row) -> Optional[str]:
        bid_tok = row["BRANCH_ID_TOKEN"]
        entity_type = row["ENTITY_TYPE"]
        slug = row["BRANCH_LABEL_SLUG"]
        if not bid_tok or not entity_type or not slug: return None
        return f"medicare_{bid_tok}_{entity_type}_{slug}"

    df["BRANCH_FEATURE_CODE"] = df.apply(_make_feature, axis=1)
    return df