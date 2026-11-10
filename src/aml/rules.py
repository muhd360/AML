from pyspark.sql import functions as F


def fan_in(txns, min_senders):
    return (txns.groupBy(F.window("txn_ts", "24 hours", "1 hour"), "to_account")
                .agg(F.approx_count_distinct("from_account").alias("n_parties"),
                     F.count("*").alias("n_txn"),
                     F.sum("amount_received").alias("total"))
                .filter(F.col("n_parties") >= min_senders)
                .select(F.lit("FAN_IN").alias("rule_id"),
                        F.col("to_account").alias("account_id"),
                        "window.start", "window.end", "n_parties", "n_txn", "total"))


def fan_out(txns, min_receivers):
    return (txns.groupBy(F.window("txn_ts", "24 hours", "1 hour"), "from_account")
                .agg(F.approx_count_distinct("to_account").alias("n_parties"),
                     F.count("*").alias("n_txn"),
                     F.sum("amount_paid").alias("total"))
                .filter(F.col("n_parties") >= min_receivers)
                .select(F.lit("FAN_OUT").alias("rule_id"),
                        F.col("from_account").alias("account_id"),
                        "window.start", "window.end", "n_parties", "n_txn", "total"))


def structuring(txns, low=9000, high=10000, min_count=3):
    cash = txns.filter((F.col("payment_format") == "Cash") &
                       (F.col("amount_paid") >= low) &
                       (F.col("amount_paid") < high))
    return (cash.groupBy(F.window("txn_ts", "24 hours", "1 hour"), "from_account")
                .agg(F.count("*").alias("n_txn"),
                     F.sum("amount_paid").alias("total"))
                .filter(F.col("n_txn") >= min_count)
                .select(F.lit("STRUCTURING").alias("rule_id"),
                        F.col("from_account").alias("account_id"),
                        "window.start", "window.end",
                        F.lit(None).cast("long").alias("n_parties"),
                        "n_txn", "total"))
