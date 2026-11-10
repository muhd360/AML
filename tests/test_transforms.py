import pytest
from pyspark.sql import SparkSession
from aml.transforms import parse, add_dq_flags


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[1]")
            .appName("aml-tests")
            .getOrCreate())


def _make_df(spark, raw_payload):
    return spark.createDataFrame(
        [(raw_payload, 0, 1, None)],
        "raw_payload string, eh_partition int, eh_offset long, ingest_ts timestamp",
    )


def test_bad_amount_is_flagged(spark):
    raw = '{"txn_id":"T1","ts":"2022/09/01 00:20","from_account":"A","to_account":"B","amount_paid":"0"}'
    out = add_dq_flags(parse(_make_df(spark, raw))).collect()[0]
    assert "bad_amount" in out.dq_errors


def test_missing_id_is_flagged(spark):
    raw = '{"ts":"2022/09/01 00:20","from_account":"A","to_account":"B","amount_paid":"100"}'
    out = add_dq_flags(parse(_make_df(spark, raw))).collect()[0]
    assert "missing_id" in out.dq_errors


def test_bad_timestamp_is_flagged(spark):
    raw = '{"txn_id":"T1","ts":"not-a-date","from_account":"A","to_account":"B","amount_paid":"100"}'
    out = add_dq_flags(parse(_make_df(spark, raw))).collect()[0]
    assert "bad_timestamp" in out.dq_errors


def test_missing_account_is_flagged(spark):
    raw = '{"txn_id":"T1","ts":"2022/09/01 00:20","amount_paid":"100"}'
    out = add_dq_flags(parse(_make_df(spark, raw))).collect()[0]
    assert "missing_account" in out.dq_errors


def test_valid_row_has_no_errors(spark):
    raw = '{"txn_id":"T1","ts":"2022/09/01 00:20","from_account":"A","to_account":"B","amount_paid":"500","amount_received":"500","payment_format":"Credit Card","payment_currency":"USD","receiving_currency":"USD","from_bank":"BK1","to_bank":"BK2","is_laundering":"0"}'
    out = add_dq_flags(parse(_make_df(spark, raw))).collect()[0]
    assert out.dq_errors == ""
