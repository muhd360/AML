from pyspark.sql import types as T

TXN_SCHEMA = T.StructType([
    T.StructField("txn_id",             T.StringType()),
    T.StructField("ts",                 T.StringType()),
    T.StructField("from_bank",          T.StringType()),
    T.StructField("from_account",       T.StringType()),
    T.StructField("to_bank",            T.StringType()),
    T.StructField("to_account",         T.StringType()),
    T.StructField("amount_received",    T.StringType()),
    T.StructField("receiving_currency", T.StringType()),
    T.StructField("amount_paid",        T.StringType()),
    T.StructField("payment_currency",   T.StringType()),
    T.StructField("payment_format",     T.StringType()),
    T.StructField("is_laundering",      T.StringType()),
])
