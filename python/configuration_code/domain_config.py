# config/domain_config.py
# ============================================================
# DOMAIN CONFIG (Single Source of Truth)
# ------------------------------------------------------------
# Contains ONLY:
# - mappings, rules, overrides, constants, routing defaults
# Contains NO:
# - DataFrame logic, no pandas operations, no I/O
# ============================================================

from __future__ import annotations
from pathlib import Path
import os

# =========================
# UVL export routing (FIXED PATH LOGIC)
# =========================
# نستخدم __file__ لضمان الوصول للمجلد الرئيسي للمشروع دائماً بغض النظر عن مكان التشغيل
BASE_DIR = Path(__file__).resolve().parent.parent
UVL_OUTPUT_DIR = BASE_DIR / "uvl_outputs"
UVL_NAMESPACE = "MedicareAuditStructure"

# =========================
# Category spelling harmonization
# =========================
CATEGORY_SPELLING_MAP = {
    "Other equipments maintenance and manulas": "Other equipments maintenance and manuals",
}

# =========================
# Branch harmonization
# =========================
BRANCH_NAME_OVERRIDES = {
    "حلحول": "34",
    "جنين - البيادر - 35": "35",
    "طوباس - المركزي Ext: 220": "32",
    "Elite Medical Consultancy Services Co. - MediCare": "headoffice",
    "Ein Sara": "33",
    "Purchase Department": "PD",
}

DEP_LABELS = {"IT", "HR", "QM", "PD", "Purchase Department", "Genetic lab"}

# =========================
# Status mappings
# IMPORTANT:
# Keys MUST be slug keys: to_uvl_code(clean_text)
# =========================
VISIT_STATUS_MAP = {
    "closed": "vs_closed",
    "pending": "vs_pending",
    "blocked": "vs_blocked",
    "in_progress": "vs_in_progress",
    "ready_and_not_identical_appeared": "vs_ready_not_identical_appeared",
    "ready_and_identical_appeared": "vs_ready_identical_appeared",
}

VISIT_RESULT_STATUS_MAP = {
    "closed": "vrs_closed",
    "opened": "vrs_opened",
    "need_department_director_approval": "vrs_need_dept_dir_approval",
    "need_branch_director_approval": "vrs_need_branch_dir_approval",
    "approved_by_branch_director": "vrs_approved_by_branch_director",
    "suggestion_approved_by_qa_manager": "vrs_sugg_approved_by_qa_manager",
    "rejection_approved_by_qa_manager": "vrs_rej_approved_by_qa_manager",
    "blocked_temporary": "vrs_blocked_temporary",
}

# =========================
# Audit type constant (used by UVL builder constraints)
# =========================
PLANNED_AUDIT_TYPE_CODE = "planned"