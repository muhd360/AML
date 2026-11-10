import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from rapidfuzz.distance import JaroWinkler


def normalise(col):
    cleaned = F.trim(F.regexp_replace(F.lower(col), "[^a-z ]", " "))
    tokens = F.array_sort(F.split(F.regexp_replace(cleaned, " +", " "), " "))
    return F.array_join(tokens, " ")  # token-sorted: handles reordered names


def block_keys(df, name_col):
    return (df.withColumn("tok", F.explode(F.split(name_col, " ")))
              .filter(F.length("tok") >= 3)
              .withColumn("bkey", F.soundex("tok")).drop("tok"))


@pandas_udf("double")
def jw(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series([JaroWinkler.similarity(x, y) for x, y in zip(a, b)])


def screen(parties, entities, min_score=0.80):
    p = block_keys(parties.withColumn("pn", normalise("name")), "pn")
    e = block_keys(entities.withColumn("en", normalise("name")), "en")
    pairs = p.join(e, "bkey").select(
        "account_id", "pn", "entity_id", "en", "list_name",
        "list_source", "entity_category"
    ).distinct()
    return (pairs.withColumn("match_score", jw("pn", "en"))
                 .filter(F.col("match_score") >= min_score))
