# Databricks notebook source
from pyspark.sql.functions import col



PII_tables = ["HONORARY_DESIGNEE"]



for table in PII_tables:
    print(table)
    rocky_table = f"empl_sensitive.refine_{table}_history"
    target_table = f"empl_sensitive.refine_{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)
