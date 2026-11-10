# Databricks notebook: Gold alert generation from rule hits + sanctions hits
from pyspark.sql import Window, functions as F

STORAGE = "<storage>"
SANCTIONS_THRESHOLD = 0.90  # match score cutoff for sanctions alerts


def to_alerts(batch_df, batch_id):
    a = (batch_df
         .withColumn("alert_date", F.to_date("start"))
         .withColumn("alert_id", F.sha2(F.concat_ws("|", "rule_id", "account_id", "alert_date"), 256))
         .withColumn("alert_type",    F.col("rule_id"))
         .withColumn("rule_version",  F.lit("v1"))
         .withColumn("status",        F.lit("OPEN"))
         .withColumn("created_ts",    F.current_timestamp())
         .withColumn("pipeline_run_id", F.lit(str(batch_id))))

    # keep strongest window per alert_id in this batch
    a = (a.withColumn("rn", F.row_number().over(
              Window.partitionBy("alert_id").orderBy(F.desc("n_txn"))))
          .filter("rn = 1")
          .drop("rn"))

    a.createOrReplaceTempView("new_alerts")
    a.sparkSession.sql("""
        MERGE INTO aml.gold.fact_alert t
        USING new_alerts s ON t.alert_id = s.alert_id
        WHEN MATCHED AND s.n_txn > t.txn_count THEN UPDATE SET
            t.txn_count    = s.n_txn,
            t.total_amount = s.total,
            t.window_start = s.start,
            t.window_end   = s.end
        WHEN NOT MATCHED THEN INSERT (
            alert_id, alert_type, rule_id, rule_version, account_id, alert_date,
            window_start, window_end, txn_count, total_amount, status, created_ts, pipeline_run_id)
        VALUES (
            s.alert_id, s.alert_type, s.rule_id, s.rule_version, s.account_id, s.alert_date,
            s.start, s.end, s.n_txn, s.total, s.status, s.created_ts, s.pipeline_run_id)
    """)

    # fill alert_transaction bridge
    txns = a.sparkSession.table("aml.silver.transactions")
    bridge = (a.join(txns,
                     (txns["from_account"] == a["account_id"]) |
                     (txns["to_account"]   == a["account_id"]))
               .filter((txns["txn_ts"] >= a["start"]) & (txns["txn_ts"] <= a["end"]))
               .select(a["alert_id"], txns["txn_id"]))
    bridge.write.mode("append").saveAsTable("aml.gold.alert_transaction")


(spark.readStream
      .table("aml.silver.rule_hits")
      .writeStream
      .foreachBatch(to_alerts)
      .option("checkpointLocation",
              f"abfss://lake@{STORAGE}.dfs.core.windows.net/_chk/gold_alerts")
      .start())
