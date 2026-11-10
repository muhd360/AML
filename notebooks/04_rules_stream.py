# Databricks notebook: Pattern detection streaming — fan-in, fan-out, structuring
# Set FAN_IN_N and FAN_OUT_N after inspecting the 99.9th percentile in a batch query.
from aml.rules import fan_in, fan_out, structuring

STORAGE  = "<storage>"
FAN_IN_N  = 10  # replace after threshold analysis
FAN_OUT_N = 10  # replace after threshold analysis

tx = (spark.readStream
           .table("aml.silver.transactions")
           .withWatermark("txn_ts", "2 hours"))

hits = (fan_in(tx, FAN_IN_N)
        .unionByName(fan_out(tx, FAN_OUT_N))
        .unionByName(structuring(tx)))

(hits.writeStream
     .outputMode("append")
     .option("checkpointLocation",
             f"abfss://lake@{STORAGE}.dfs.core.windows.net/_chk/rule_hits")
     .toTable("aml.silver.rule_hits"))
