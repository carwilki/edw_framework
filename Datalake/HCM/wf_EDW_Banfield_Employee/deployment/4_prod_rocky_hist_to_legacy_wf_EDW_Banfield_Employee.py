# Databricks notebook source
from pyspark.sql.functions import col


pii_tables = ["BANFIELD_EMPLOYEE"]


for table in pii_tables:
    rocky_table = f"refine.{table}_history"
    target_table = f"empl_sensitive.refine_{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)
