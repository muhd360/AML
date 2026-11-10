# Databricks notebook: Evaluate screening precision/recall and pattern detection
# Run after the full pipeline has processed.

from pyspark.sql import functions as F

# --- Sanctions screening threshold sweep ---
truth = spark.table("aml.ref.planted_truth")
hits  = spark.table("aml.silver.screening_hits")

print("=== Sanctions screening threshold sweep ===")
for th in [0.85, 0.88, 0.90, 0.92, 0.95]:
    flagged = hits.filter(f"match_score >= {th}").select("account_id").distinct()
    tp = flagged.join(truth, "account_id").count()
    fp = flagged.count() - tp
    fn = truth.count() - tp
    prec = tp / (tp + fp) if (tp + fp) > 0 else 0
    rec  = tp / (tp + fn) if (tp + fn) > 0 else 0
    print(f"  threshold={th}  precision={prec:.1%}  recall={rec:.1%}  tp={tp} fp={fp} fn={fn}")

# --- Recall by name variant ---
print("\n=== Recall by variant (threshold=0.90) ===")
flagged_90 = hits.filter("match_score >= 0.90").select("account_id").distinct()
variant_recall = (truth.join(flagged_90, "account_id", "left")
                       .withColumn("hit", F.col("account_id").isNotNull().cast("int"))
                       .groupBy("variant")
                       .agg(F.sum("hit").alias("tp"), F.count("*").alias("total"))
                       .withColumn("recall", F.col("tp") / F.col("total")))
variant_recall.show()

# --- Decoy false-positive rate ---
decoys = spark.table("aml.ref.decoys")
fp_decoys = flagged_90.join(decoys, "account_id").count()
print(f"\nDecoys flagged at 0.90: {fp_decoys} / {decoys.count()}")

# --- Pattern detection (transaction-level) ---
print("\n=== Pattern detection ===")
alert_txns = spark.table("aml.gold.alert_transaction")
silver     = spark.table("aml.silver.transactions")
flagged_txns = alert_txns.join(silver, "txn_id").select("txn_id", "is_laundering").distinct()
tp_p = flagged_txns.filter("is_laundering = 1").count()
fp_p = flagged_txns.filter("is_laundering = 0").count()
all_laund = silver.filter("is_laundering = 1").count()
fn_p = all_laund - tp_p
prec_p = tp_p / (tp_p + fp_p) if (tp_p + fp_p) > 0 else 0
rec_p  = tp_p / (tp_p + fn_p) if (tp_p + fn_p) > 0 else 0
print(f"  precision={prec_p:.1%}  recall={rec_p:.1%}")

# --- Alerts per day ---
alerts = spark.table("aml.gold.fact_alert")
n_alerts = alerts.count()
date_range = silver.agg(
    F.datediff(F.max("txn_ts"), F.min("txn_ts")).alias("days")
).collect()[0]["days"]
print(f"\nAlerts per day: {n_alerts / max(date_range, 1):.1f}  (total={n_alerts}, days={date_range})")
print("\nRecord all numbers in results.md before closing this notebook.")
