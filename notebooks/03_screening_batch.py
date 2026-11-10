# Databricks notebook: Sanctions + PEPs screening batch job
# Run after 03a_synthetic_names.py and after OpenSanctions files are uploaded to the volume.
from pyspark.sql import functions as F
from aml.screening import screen

LIST_VERSION = "2026-11-10"  # update to the actual download date you recorded

ents_sanctions = (
    spark.read.option("header", True)
         .csv("/Volumes/aml/ref/raw/sanctions_targets.simple.csv")
         .selectExpr(
             "id AS entity_id", "name", "name AS list_name",
             "'OpenSanctions sanctions' AS list_source",
             "'SANCTION' AS entity_category",
         )
)

ents_peps = (
    spark.read.option("header", True)
         .csv("/Volumes/aml/ref/raw/peps.simple.csv")
         .selectExpr(
             "id AS entity_id", "name", "name AS list_name",
             "'OpenSanctions PEPs' AS list_source",
             "'PEP' AS entity_category",
         )
)

ents = ents_sanctions.unionByName(ents_peps)

hits = screen(spark.table("aml.ref.account_names"), ents)

(hits
 .withColumn("list_version", F.lit(LIST_VERSION))
 .withColumn("screened_at", F.current_timestamp())
 .write.mode("overwrite")
 .saveAsTable("aml.silver.screening_hits"))

print("Screening complete. Hits:", hits.count())
