# Databricks notebook: Silver streaming — parse, type, deduplicate, quarantine
# Requires aml.silver.transactions to be created first with the same columns as 'good'.
from aml.transforms import parse, add_dq_flags

STORAGE = "<storage>"


def write_batch(batch_df, batch_id):
    checked = add_dq_flags(parse(batch_df)).cache()
    good = checked.filter("dq_errors = ''").drop("dq_errors", "raw_payload")
    bad  = checked.filter("dq_errors != ''")

    good.createOrReplaceTempView("incoming")
    good.sparkSession.sql("""
        MERGE INTO aml.silver.transactions t
        USING (
            SELECT * FROM incoming
            QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY ingest_ts) = 1
        ) s
        ON t.txn_id = s.txn_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    bad.write.mode("append").saveAsTable("aml.silver.transactions_quarantine")
    checked.unpersist()


(spark.readStream.table("aml.bronze.transactions_raw")
   .writeStream
   .foreachBatch(write_batch)
   .option("checkpointLocation",
           f"abfss://lake@{STORAGE}.dfs.core.windows.net/_chk/silver_txn")
   .trigger(processingTime="30 seconds")
   .start())
