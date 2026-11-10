from pyspark.sql import functions as F, types as T

TXN_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in [
    "txn_id", "ts", "from_bank", "from_account", "to_bank", "to_account",
    "amount_received", "receiving_currency", "amount_paid", "payment_currency",
    "payment_format", "is_laundering"]])


def parse(df):
    j = F.from_json("raw_payload", TXN_SCHEMA)
    return (df.withColumn("j", j).select(
        "j.txn_id",
        F.to_timestamp("j.ts", "yyyy/MM/dd HH:mm").alias("txn_ts"),
        "j.from_bank", "j.from_account", "j.to_bank", "j.to_account",
        F.col("j.amount_paid").cast("decimal(18,2)").alias("amount_paid"),
        "j.payment_currency",
        F.col("j.amount_received").cast("decimal(18,2)").alias("amount_received"),
        "j.receiving_currency", "j.payment_format",
        F.col("j.is_laundering").cast("int").alias("is_laundering"),
        "raw_payload", "eh_partition", "eh_offset", "ingest_ts"))


def add_dq_flags(df):
    reasons = F.concat_ws(",",
        F.when(F.col("txn_id").isNull(), F.lit("missing_id")),
        F.when(F.col("txn_ts").isNull(), F.lit("bad_timestamp")),
        F.when(F.col("amount_paid").isNull() | (F.col("amount_paid") <= 0), F.lit("bad_amount")),
        F.when(F.col("from_account").isNull() | F.col("to_account").isNull(), F.lit("missing_account")))
    return df.withColumn("dq_errors", reasons)
