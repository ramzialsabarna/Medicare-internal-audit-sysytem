import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import linregress

# =========================
# CONFIG
# =========================
BASE_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit"
RESULTS_FILE = os.path.join(BASE_DIR, "structurecode", "final_phd_validation_results_sat.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "structurecode", "phd_final_charts_v2")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# STYLE (publication-friendly)
# =========================
plt.rcParams.update({
    "font.size": 11,
    "font.family": "serif"
})

def save_fig(name: str):
    """Save both PNG and PDF for paper-ready usage."""
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, name + ".png"), dpi=300, bbox_inches="tight")
    plt.savefig(os.path.join(OUTPUT_DIR, name + ".pdf"), bbox_inches="tight")
    plt.close()

# =========================
# LOAD + VALIDATE
# =========================
df = pd.read_csv(RESULTS_FILE)

required_cols = ["Group","Model","Root","NF","Constraints","SAT","N_Dead","N_FalseOptional","TimeSec","Defects"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"Missing required columns in CSV: {missing}")

# =========================
# NORMALIZE
# =========================
def norm_group(g: str) -> str:
    s = str(g).lower()
    if "original" in s or "clean" in s:
        return "Clean"
    if "inject" in s or "scientific" in s or "defect" in s:
        return "Injected"
    return str(g)

df["Group_std"] = df["Group"].apply(norm_group)
df["TimeMs"] = df["TimeSec"].astype(float) * 1000.0
df["MsPerFeature"] = df["TimeMs"] / df["NF"].replace(0, np.nan)

# Extract complexity level (c1..c5) from model name
lvl = df["Model"].astype(str).str.extract(r"(c\d)")
df["Level"] = lvl[0].fillna("unknown")

# normalize Defects
df["Defects_norm"] = df["Defects"].fillna("None").astype(str).str.strip()
df["SAT_norm"] = df["SAT"].fillna("").astype(str).str.strip().str.upper()

# Prediction rule: any detected defect OR unsat
df["PredDefective"] = (
    (df["N_Dead"].fillna(0) > 0) |
    (df["N_FalseOptional"].fillna(0) > 0) |
    (df["SAT_norm"] != "SAT") |
    (df["Defects_norm"].str.lower() != "none")
)

# Ground truth from Group
df["ActualDefective"] = (df["Group_std"] == "Injected")

# Known sampling artifact: FO:AuditPlan appearing in "Clean"
df["Artifact_FO_AuditPlan"] = (
    (df["Group_std"] == "Clean") &
    (df["Defects_norm"].str.contains(r"FO:AuditPlan", case=False, na=False))
)

# =========================
# METRICS (raw + filtered)
# =========================
def confusion_metrics(frame: pd.DataFrame, title: str):
    y_true = frame["ActualDefective"].astype(bool).values
    y_pred = frame["PredDefective"].astype(bool).values

    tp = int(((y_true == True) & (y_pred == True)).sum())
    tn = int(((y_true == False) & (y_pred == False)).sum())
    fp = int(((y_true == False) & (y_pred == True)).sum())
    fn = int(((y_true == True) & (y_pred == False)).sum())

    acc = (tp + tn) / max(1, (tp + tn + fp + fn))
    prec = tp / max(1, (tp + fp))
    rec = tp / max(1, (tp + fn))
    f1 = (2 * prec * rec) / max(1e-12, (prec + rec))

    print("\n" + "="*80)
    print(title)
    print(f"TN={tn}, FP={fp}, FN={fn}, TP={tp}")
    print(f"Accuracy={acc:.3f}, Precision={prec:.3f}, Recall={rec:.3f}, F1={f1:.3f}")
    print("="*80)

    return {"TN":tn,"FP":fp,"FN":fn,"TP":tp,"Accuracy":acc,"Precision":prec,"Recall":rec,"F1":f1}

raw_metrics = confusion_metrics(df, "RAW EVALUATION (includes sampling artifacts)")
df_filtered = df[~df["Artifact_FO_AuditPlan"]].copy()
filtered_metrics = confusion_metrics(df_filtered, "FILTERED EVALUATION (excludes Clean FO:AuditPlan artifacts)")

# Export metrics summary
summary = pd.DataFrame([{"Mode":"RAW", **raw_metrics}, {"Mode":"FILTERED", **filtered_metrics}])
summary.to_csv(os.path.join(OUTPUT_DIR, "summary_metrics.csv"), index=False, encoding="utf-8-sig")

# Print problematic cases explicitly
fp_cases = df[(df["Group_std"]=="Clean") & (df["PredDefective"]==True)][["Model","Level","Defects_norm","N_Dead","N_FalseOptional","SAT_norm"]]
fn_cases = df[(df["Group_std"]=="Injected") & (df["PredDefective"]==False)][["Model","Level","Defects_norm","N_Dead","N_FalseOptional","SAT_norm"]]

fp_cases.to_csv(os.path.join(OUTPUT_DIR, "false_positives_clean.csv"), index=False, encoding="utf-8-sig")
fn_cases.to_csv(os.path.join(OUTPUT_DIR, "false_negatives_injected.csv"), index=False, encoding="utf-8-sig")

print(f"\nSaved FP cases: {os.path.join(OUTPUT_DIR, 'false_positives_clean.csv')}")
print(f"Saved FN cases: {os.path.join(OUTPUT_DIR, 'false_negatives_injected.csv')}")

# =========================
# FIGURE 1: Scalability scatter + regression
# =========================
plt.figure(figsize=(8,5))
for g, marker in [("Clean","o"), ("Injected","s")]:
    sub = df[df["Group_std"]==g].sort_values("NF")
    plt.scatter(sub["NF"], sub["TimeMs"], alpha=0.7, label=g)

# regressions
for g in ["Clean","Injected"]:
    sub = df[df["Group_std"]==g]
    lr = linregress(sub["NF"], sub["TimeMs"])
    xs = np.linspace(sub["NF"].min(), sub["NF"].max(), 100)
    ys = lr.intercept + lr.slope * xs
    plt.plot(xs, ys, linewidth=2, label=f"{g} fit (R²={lr.rvalue**2:.2f})")

plt.xlabel("Number of Features (NF)")
plt.ylabel("Reasoning Time (ms)")
plt.title("Reasoning Scalability: Time vs Model Size")
plt.grid(True, linestyle="--", alpha=0.4)
plt.legend()
save_fig("01_scalability_scatter_regression")

# =========================
# FIGURE 2: Confusion matrix (RAW)
# =========================
def plot_confusion(metrics: dict, name: str, title: str):
    tn, fp, fn, tp = metrics["TN"], metrics["FP"], metrics["FN"], metrics["TP"]
    mat = np.array([[tn, fp],[fn, tp]])

    plt.figure(figsize=(6,4))
    plt.imshow(mat)
    plt.xticks([0,1], ["Pred Clean","Pred Defective"])
    plt.yticks([0,1], ["Actual Clean","Actual Injected"])
    for i in range(2):
        for j in range(2):
            plt.text(j, i, str(mat[i,j]), ha="center", va="center", fontsize=14)
    plt.title(f"{title}\nAcc={metrics['Accuracy']:.3f}, Prec={metrics['Precision']:.3f}, Rec={metrics['Recall']:.3f}")
    plt.tight_layout()
    save_fig(name)

plot_confusion(raw_metrics, "02_confusion_raw", "Detection Confusion Matrix (RAW)")
plot_confusion(filtered_metrics, "03_confusion_filtered", "Detection Confusion Matrix (FILTERED)")

# =========================
# FIGURE 4: Efficiency (ms/feature) vs NF
# =========================
plt.figure(figsize=(8,5))
for g in ["Clean","Injected"]:
    sub = df[df["Group_std"]==g]
    plt.scatter(sub["NF"], sub["MsPerFeature"], alpha=0.7, label=g)
plt.xlabel("Number of Features (NF)")
plt.ylabel("Time per Feature (ms/feature)")
plt.title("Reasoning Efficiency: ms per Feature")
plt.grid(True, linestyle="--", alpha=0.4)
plt.legend()
save_fig("04_efficiency_ms_per_feature")

# =========================
# FIGURE 5: Execution time distribution (boxplot)
# =========================
plt.figure(figsize=(7,5))
data_clean = df[df["Group_std"]=="Clean"]["TimeMs"].values
data_inj = df[df["Group_std"]=="Injected"]["TimeMs"].values
plt.boxplot([data_clean, data_inj], labels=["Clean","Injected"])
plt.xlabel("Group")
plt.ylabel("Reasoning Time (ms)")
plt.title("Execution Time Distribution by Group")
plt.grid(True, linestyle="--", alpha=0.4)
save_fig("05_time_boxplot")

# =========================
# FIGURE 6: Paired delta time (Injected - Clean)
# =========================
def base_model_key(m: str) -> str:
    # remove known injected prefixes like SCIENTIFIC_V5_
    return re.sub(r"^SCIENTIFIC_V\\d+_", "", str(m))

tmp = df.copy()
tmp["BaseKey"] = tmp["Model"].apply(base_model_key)

clean = tmp[tmp["Group_std"]=="Clean"][["BaseKey","NF","TimeMs"]].rename(columns={"TimeMs":"TimeMs_Clean"})
inj   = tmp[tmp["Group_std"]=="Injected"][["BaseKey","NF","TimeMs"]].rename(columns={"TimeMs":"TimeMs_Injected"})

paired = pd.merge(clean, inj, on=["BaseKey","NF"], how="inner")
paired["DeltaMs"] = paired["TimeMs_Injected"] - paired["TimeMs_Clean"]
paired.to_csv(os.path.join(OUTPUT_DIR, "paired_delta_time.csv"), index=False, encoding="utf-8-sig")

plt.figure(figsize=(8,5))
plt.scatter(paired["NF"], paired["DeltaMs"], alpha=0.8)
plt.axhline(0, linestyle="--", alpha=0.6)
plt.xlabel("Number of Features (NF)")
plt.ylabel("ΔTime (Injected - Clean) ms")
plt.title("Paired Overhead of Injection on Reasoning Time")
plt.grid(True, linestyle="--", alpha=0.4)
save_fig("06_paired_delta_time")

# =========================
# FIGURE 7: Defect type frequency (from Defects text)
# =========================
def extract_defect_types(s: str):
    if not s or s.lower()=="none":
        return []
    parts = [p.strip() for p in s.split(";") if p.strip()]
    types = []
    for p in parts:
        if ":" in p:
            types.append(p.split(":",1)[0].strip())
        else:
            types.append(p.strip())
    return types

inj_def = df[df["Group_std"]=="Injected"].copy()
inj_def["Types"] = inj_def["Defects_norm"].apply(extract_defect_types)
type_counts = pd.Series([t for row in inj_def["Types"] for t in row]).value_counts().head(10)

plt.figure(figsize=(8,5))
plt.bar(type_counts.index.astype(str), type_counts.values)
plt.xlabel("Defect Type (top-10)")
plt.ylabel("Count")
plt.title("Most Frequent Detected Defect Types (Injected)")
plt.grid(True, axis="y", linestyle="--", alpha=0.4)
save_fig("07_defect_type_frequency_top10")

# =========================
# FIGURE 8: Mean reasoning time by level and group (with SE)
# =========================
plt.figure(figsize=(8,5))
levels = sorted([x for x in df["Level"].unique() if x != "unknown"])
x = np.arange(len(levels))
width = 0.35

means_clean = []
ses_clean = []
means_inj = []
ses_inj = []

for lv in levels:
    c = df[(df["Level"]==lv) & (df["Group_std"]=="Clean")]["TimeMs"]
    i = df[(df["Level"]==lv) & (df["Group_std"]=="Injected")]["TimeMs"]
    means_clean.append(c.mean())
    ses_clean.append(c.std(ddof=1)/np.sqrt(max(1,len(c))))
    means_inj.append(i.mean())
    ses_inj.append(i.std(ddof=1)/np.sqrt(max(1,len(i))))

plt.bar(x - width/2, means_clean, width, yerr=ses_clean, label="Clean")
plt.bar(x + width/2, means_inj, width, yerr=ses_inj, label="Injected")
plt.xticks(x, levels)
plt.xlabel("Complexity Level")
plt.ylabel("Mean Reasoning Time (ms)")
plt.title("Mean Reasoning Time by Level and Group (±SE)")
plt.grid(True, axis="y", linestyle="--", alpha=0.4)
plt.legend()
save_fig("08_mean_time_by_level_group")

# =========================
# FIGURE 9: Cumulative reasoning load (sorted by NF)
# =========================
plt.figure(figsize=(8,5))
for g in ["Clean","Injected"]:
    sub = df[df["Group_std"]==g].sort_values("NF")
    cum = sub["TimeMs"].cumsum().values
    plt.plot(np.arange(1, len(cum)+1), cum, marker="o", label=g)
plt.xlabel("Models (sorted by NF)")
plt.ylabel("Cumulative Reasoning Time (ms)")
plt.title("Cumulative Reasoning Load")
plt.grid(True, linestyle="--", alpha=0.4)
plt.legend()
save_fig("09_cumulative_reasoning_load")

# =========================
# FIGURE 10: Dead/FalseOptional counts overview
# =========================
plt.figure(figsize=(8,5))
sub = df.copy()
sub["TotalDefectsCount"] = sub["N_Dead"].fillna(0) + sub["N_FalseOptional"].fillna(0)
for g in ["Clean","Injected"]:
    gg = sub[sub["Group_std"]==g]
    plt.scatter(gg["NF"], gg["TotalDefectsCount"], alpha=0.7, label=g)
plt.xlabel("Number of Features (NF)")
plt.ylabel("N_Dead + N_FalseOptional")
plt.title("Detected Defects Count vs Model Size")
plt.grid(True, linestyle="--", alpha=0.4)
plt.legend()
save_fig("10_defect_counts_vs_nf")

print("\n✅ Done.")
print(f"Charts + reports saved in: {OUTPUT_DIR}")
