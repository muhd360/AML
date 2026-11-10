# Databricks notebook: generate synthetic party names and plant 500 sanctioned names as ground truth
# Run once after Silver is populated.
import random
import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)  # fixed seeds: same names on every rerun

t = spark.table("aml.silver.transactions")
accts = (t.selectExpr("from_account AS account_id")
          .union(t.selectExpr("to_account"))
          .distinct()
          .toPandas())
accts["name"] = [
    fake.company() if random.random() < 0.15 else fake.name()
    for _ in range(len(accts))
]

sanc = pd.read_csv("/Volumes/aml/ref/raw/sanctions_targets.simple.csv")
people = sanc[sanc["schema"] == "Person"].sample(500, random_state=42)

TRANSLIT = {
    "mohammed": "muhammad", "muhammad": "mohamed", "yusuf": "yousef",
    "sergei": "sergey", "aleksandr": "alexander", "abdullah": "abdallah",
}


def typo(n):
    i = random.randrange(1, max(2, len(n) - 1))
    return n[:i] + n[i + 1:]


def reorder(n):
    return " ".join(n.split()[::-1])


def drop_mid(n):
    p = n.split()
    return f"{p[0]} {p[-1]}" if len(p) > 2 else n


def translit(n):
    return " ".join(TRANSLIT.get(w.lower(), w) for w in n.split())


VARIANTS = [
    ("exact",           lambda n: n),
    ("typo",            typo),
    ("reorder",         reorder),
    ("drop_middle",     drop_mid),
    ("transliteration", translit),
]

targets = accts.sample(800, random_state=7)  # 500 planted + 300 decoys
truth, decoys = [], []

for (idx, acct), (_, ent), k in zip(
    targets.iloc[:500].iterrows(), people.iterrows(), range(500)
):
    label, fn = VARIANTS[k % len(VARIANTS)]
    planted = fn(ent["name"])
    accts.loc[idx, "name"] = planted
    truth.append((acct["account_id"], ent["id"], ent["name"], planted, label))

for (idx, acct), (_, ent) in zip(
    targets.iloc[500:].iterrows(),
    people.sample(300, random_state=9).iterrows()
):
    decoy = f"{fake.first_name()} {ent['name'].split()[-1]}"
    accts.loc[idx, "name"] = decoy
    decoys.append((acct["account_id"], decoy))

spark.createDataFrame(accts).write.mode("overwrite").saveAsTable("aml.ref.account_names")
spark.createDataFrame(
    truth,
    "account_id string, entity_id string, list_name string, planted_name string, variant string"
).write.mode("overwrite").saveAsTable("aml.ref.planted_truth")
spark.createDataFrame(
    decoys, "account_id string, decoy_name string"
).write.mode("overwrite").saveAsTable("aml.ref.decoys")
