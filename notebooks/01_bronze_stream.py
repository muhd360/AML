# Databricks notebook: Bronze streaming ingestion from Event Hubs via Kafka endpoint
# Replace <storage> with your ADLS Gen2 storage account name.
from pyspark.sql import functions as F

STORAGE = "<storage>"  # e.g. "amllakestorage"
EH_NS   = "aml-eventhubs"

conn = dbutils.secrets.get("aml", "eh-listen")
opts = {
    "kafka.bootstrap.servers":  f"{EH_NS}.servicebus.windows.net:9093",
    "subscribe":                "transactions",
    "kafka.group.id":           "databricks",
    "kafka.security.protocol":  "SASL_SSL",
    "kafka.sasl.mechanism":     "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f'required username="$ConnectionString" password="{conn}";'
    ),
    "startingOffsets":          "earliest",
    "maxOffsetsPerTrigger":     50000,
}

(spark.readStream.format("kafka").options(**opts).load()
   .select(
       F.col("value").cast("string").alias("raw_payload"),
       F.col("key").cast("string").alias("event_key"),
       F.col("partition").alias("eh_partition"),
       F.col("offset").alias("eh_offset"),
       F.col("timestamp").alias("eh_enqueued_ts"),
       F.current_timestamp().alias("ingest_ts"))
   .withColumn("ingest_date", F.to_date("ingest_ts"))
   .writeStream
   .option("checkpointLocation",
           f"abfss://lake@{STORAGE}.dfs.core.windows.net/_chk/bronze_txn")
   .partitionBy("ingest_date")
   .trigger(processingTime="30 seconds")
   .toTable("aml.bronze.transactions_raw"))
