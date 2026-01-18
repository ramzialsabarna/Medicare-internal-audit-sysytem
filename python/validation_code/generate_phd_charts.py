import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import linregress

# ===================== PATHS =====================
BASE_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit"
RESULTS_FILE = os.path.join(BASE_DIR, "structurecode", "final_phd_validation_results_sat.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "structurecode", "phd_final_charts_v2")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===================== HELPERS =====================
def normalize_group(x: str) -> str:
    """
    Normalize group labels into two canonical classes: Clean vs Injected
    Works with common labels:
      - "Original (Clean)", "Original", "Clean", "Baseline"
      - "Injected (Scientific)", "Injected", "Defective", "Scientific"
    """
    if pd.isna(x):
        return "Unknown"
    s = str(x).strip().lower()
    if "original" in s or "clean" in s or "baseline" in s:
        return "Clean"
    if "injected" in s or "defect" in s or "scientific" in s:
        return "Injected"
    return str(x).strip()

def extract_level(model_name: str) -> str:
    # Extract c1..c5 from model filename like Reduced_c3_M01...
    if pd.isna(model_name):
        return np.nan
    m = re.search(r"(c\d)", str(model_name))
    return m.group(1) if m else np.nan

def save_plot(name_base: str):
    plt.tight_layout()
    plt.grid(True, linestyle="--", alpha=0.4)
    png_path = os.path.join(OUTPUT_DIR, f"{name_base}.png")
    pdf_path = os.path.join(OUTPUT_DIR, f"{name_base}.pdf")
    plt.savefig(png_path, dpi=300, bbox_inches="tight")
    plt.savefig(pdf_path, bbox_inches="tight")
    plt.close()

def safe_num(s):
    return pd.to_numeric(s, errors="coerce")

# ===================== LOAD =====================
df_raw = pd.read_csv(RESULTS_FILE)

required = ["Group", "Model", "NF", "TimeSec", "SAT", "N_Dead", "N_FalseOptional"]
missing = [c for c in required if c not in df_raw.columns]
if missing:
    raise ValueError(f"Missing required columns: {missing}. Found: {list(df_raw.columns)}")

df = df_raw.copy()

# Standardize
df["Group_std"] = df["Group"].apply(normalize_group)
df["Model_std"] = df["Model"].astype(str)
df["Level"] = df["Model_std"].apply(extract_level)

df["NF_std"] = safe_num(df["NF"])
df["TimeSec_std"] = safe_num(df["TimeSec"])
df["Time_ms"] = df["TimeSec_std"] * 1000.0

df["N_Dead_std"] = safe_num(df["N_Dead"]).fillna(0).astype(int)
df["N_FO_std"] = safe_num(df["N_FalseOptional"]).fillna(0).astype(int)

# Define "predicted defective" in a publication-meaningful way:
# - UNSAT => defective
# - or any logical defect counts (dead or false optional)
sat_low = df["SAT"].astype(str).str.lower()
df["UNSAT_flag"] = sat_low.str.contains("unsat").astype(int)

df["NDF_std"] = df["N_Dead_std"] + df["N_FO_std"] + df["UNSAT_flag"]
df["PredDefective"] = (df["NDF_std"] > 0).astype(int)

# Efficiency metric
df = df.dropna(subset=["NF_std", "TimeSec_std"])
df = df[df["NF_std"] > 0]
df["ms_per_feature"] = (df["Time_ms"] / df["NF_std"]).replace([np.inf, -np.inf], np.nan)

# ===================== FIG 1: Scalability (Time vs NF) =====================
plt.figure(figsize=(8, 5))
for g in ["Clean", "Injected"]:
    sub = df[df["Group_std"] == g].sort_values("NF_std")
    if len(sub) == 0:
        continue
    plt.plot(sub["NF_std"], sub["Time_ms"], marker="s", linestyle="-", label=g)
plt.xlabel("Number of Features (NF)")
plt.ylabel("Reasoning Time (ms)")
plt.title("Reasoning Scalability: Time vs Model Size")
plt.legend()
save_plot("01_scalability_time_vs_nf")

# ===================== FIG 2: Confusion Matrix (Clean vs Injected) =====================
# Ground truth from Group_std (binary classification)
gt = df["Group_std"].map({"Clean": 0, "Injected": 1}).fillna(-1).astype(int)
pred = df["PredDefective"].astype(int)

mask = gt.isin([0, 1])
gt2 = gt[mask]
pred2 = pred[mask]

TN = int(((gt2 == 0) & (pred2 == 0)).sum())
FP = int(((gt2 == 0) & (pred2 == 1)).sum())
FN = int(((gt2 == 1) & (pred2 == 0)).sum())
TP = int(((gt2 == 1) & (pred2 == 1)).sum())

acc = (TP + TN) / max((TP + TN + FP + FN), 1)
precision = TP / max((TP + FP), 1)
recall = TP / max((TP + FN), 1)

plt.figure(figsize=(6.6, 4.6))
plt.axis("off")
table_data = [
    ["", "Pred Clean", "Pred Defective"],
    ["Actual Clean", f"{TN}", f"{FP}"],
    ["Actual Injected", f"{FN}", f"{TP}"],
]
tbl = plt.table(cellText=table_data, cellLoc="center", loc="center")
tbl.auto_set_font_size(False)
tbl.set_fontsize(11)
tbl.scale(1.2, 1.6)
plt.title(
    f"Detection Confusion Matrix\nAccuracy={acc:.3f}, Precision={precision:.3f}, Recall={recall:.3f}",
    pad=20
)
save_plot("02_confusion_matrix")

# ===================== FIG 3: Efficiency (ms per Feature) =====================
plt.figure(figsize=(8, 5))
for g in ["Clean", "Injected"]:
    sub = df[df["Group_std"] == g]
    if len(sub) == 0:
        continue
    plt.scatter(sub["NF_std"], sub["ms_per_feature"], label=g, alpha=0.7)
plt.xlabel("Number of Features (NF)")
plt.ylabel("Time per Feature (ms/feature)")
plt.title("Reasoning Efficiency: ms per Feature")
plt.legend()
save_plot("03_efficiency_ms_per_feature")

# ===================== FIG 4: Distribution of NF =====================
plt.figure(figsize=(8, 5))
for g in ["Clean", "Injected"]:
    sub = df[df["Group_std"] == g]
    if len(sub) == 0:
        continue
    plt.hist(sub["NF_std"], bins=15, alpha=0.6, label=g)
plt.xlabel("Number of Features (NF)")
plt.ylabel("Count of Models")
plt.title("Distribution of Model Sizes (NF)")
plt.legend()
save_plot("04_distribution_nf")

# ===================== FIG 5: Regression (Time vs NF) with R² =====================
plt.figure(figsize=(8, 5))
for g in ["Clean", "Injected"]:
    sub = df[df["Group_std"] == g]
    if len(sub) < 2:
        continue
    x = sub["NF_std"].values
    y = sub["Time_ms"].values

    lr = linregress(x, y)
    yhat = lr.intercept + lr.slope * x
    r2 = lr.rvalue ** 2

    plt.scatter(x, y, alpha=0.5, label=f"{g} (R²={r2:.2f})")
    order = np.argsort(x)
    plt.plot(x[order], yhat[order], linestyle="-")

plt.xlabel("Number of Features (NF)")
plt.ylabel("Reasoning Time (ms)")
plt.title("Performance Regression: Time vs NF")
plt.legend()
save_plot("05_regression_time_vs_nf")

# ===================== FIG 6: Boxplot (Time by Group) =====================
plt.figure(figsize=(7.6, 5))
groups = ["Clean", "Injected"]
data = [df[df["Group_std"] == g]["Time_ms"].dropna().values for g in groups]
plt.boxplot(data, labels=groups, showfliers=False)
plt.xlabel("Group")
plt.ylabel("Reasoning Time (ms)")
plt.title("Execution Time Variance by Group")
save_plot("06_boxplot_time")

# ===================== FIG 7: Complexity Levels (c1-c5) Mean NF by Group =====================
plt.figure(figsize=(8, 5))
levels = [f"c{i}" for i in range(1, 6)]
xpos = np.arange(len(levels))
width = 0.35

means_clean = []
means_inj = []
for lv in levels:
    means_clean.append(df[(df["Level"] == lv) & (df["Group_std"] == "Clean")]["NF_std"].mean())
    means_inj.append(df[(df["Level"] == lv) & (df["Group_std"] == "Injected")]["NF_std"].mean())

means_clean = np.nan_to_num(means_clean, nan=0.0)
means_inj = np.nan_to_num(means_inj, nan=0.0)

plt.bar(xpos - width/2, means_clean, width, label="Clean")
plt.bar(xpos + width/2, means_inj, width, label="Injected")
plt.xticks(xpos, levels)
plt.xlabel("Complexity Level (c1–c5)")
plt.ylabel("Mean Number of Features (NF)")
plt.title("Complexity Levels vs Model Size (NF)")
plt.legend()
save_plot("07_levels_mean_nf")

# ===================== FIG 8: Cumulative Reasoning Load =====================
plt.figure(figsize=(8, 5))
for g in ["Clean", "Injected"]:
    sub = df[df["Group_std"] == g].sort_values("NF_std")
    if len(sub) == 0:
        continue
    cum = sub["Time_ms"].cumsum()
    plt.plot(range(1, len(cum) + 1), cum.values, marker="o", linestyle="-", label=g)
plt.xlabel("Models (sorted by NF)")
plt.ylabel("Cumulative Reasoning Time (ms)")
plt.title("Cumulative Reasoning Load")
plt.legend()
save_plot("08_cumulative_reasoning_load")

# ===================== FIG 9: Violin plot (ms/feature by Group) =====================
plt.figure(figsize=(7.6, 5))
vals_clean = df[df["Group_std"] == "Clean"]["ms_per_feature"].dropna().values
vals_inj = df[df["Group_std"] == "Injected"]["ms_per_feature"].dropna().values
plt.violinplot([vals_clean, vals_inj], showmeans=True, showextrema=True)
plt.xticks([1, 2], ["Clean", "Injected"])
plt.xlabel("Group")
plt.ylabel("Time per Feature (ms/feature)")
plt.title("Performance Gap: ms/Feature Distribution")
save_plot("09_violin_ms_per_feature")

# ===================== FIG 10: Summary Table (Mean Time by Level x Group) =====================
pivot = df.pivot_table(values="Time_ms", index="Level", columns="Group_std", aggfunc="mean")
pivot = pivot.reindex(levels)

plt.figure(figsize=(8.4, 3.9))
plt.axis("off")

cell_text = []
for lv in pivot.index:
    row = [lv]
    for g in ["Clean", "Injected"]:
        v = pivot.loc[lv, g] if g in pivot.columns else np.nan
        row.append("" if pd.isna(v) else f"{v:.3f}")
    cell_text.append(row)

table = plt.table(
    cellText=[["Level", "Mean Time (ms) - Clean", "Mean Time (ms) - Injected"]] + cell_text,
    cellLoc="center",
    loc="center"
)
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1.1, 1.5)

plt.title("Mean Reasoning Time by Complexity Level and Group", pad=15)
save_plot("10_summary_table_level_group")

print(f"✅ 10 publication-ready charts generated in: {OUTPUT_DIR}")
