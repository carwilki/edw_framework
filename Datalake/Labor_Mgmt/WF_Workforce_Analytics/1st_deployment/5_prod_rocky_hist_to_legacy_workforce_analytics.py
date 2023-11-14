# Databricks notebook source
from pyspark.sql.functions import col

tables = [
    "WFA_LBRACCT",
    "WFA_ORG",
    "WFA_FCST_SLS_DEPT",
    "WFA_EMP",
    "WFA_PAYCD",
    "WFA_FCST_SLS_TASK",
]

PII_tables = ["WFA_TSCHD", "WFA_TDTL"]

for table in tables:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"legacy.{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)


for table in PII_tables:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"empl_protected.legacy_{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)
