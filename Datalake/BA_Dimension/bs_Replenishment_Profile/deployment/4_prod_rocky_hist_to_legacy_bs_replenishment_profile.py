# Databricks notebook source
from pyspark.sql.functions import col
import csv


tables = ["SITE_GROUP_DAY"."REPLENISHMENT_PROFILE","REPLENISHMENT_DAY","DC_FCST_DAY"]
tables_pre=["UOM_ROUNDING_RULE_PRE","ROUNDING_PROFILE_PRE"]


for table in tables:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"legacy.{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)


for table in tables_pre:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"raw.{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)
