# Databricks notebook source
from pyspark.sql.functions import col

tables = [
    "GL_DOC_TYPE",
    "GL_CATEGORY",
    "BAL_FILE_INTRFACE",
    "GL_PROFIT_CENTER",
    "SAP_CATEGORY",
    "GL_PLAN_FORECAST_MONTH",
    "BAL_FILE_INTRFACE_TXT",
    "GL_ACTUAL_DAY_DETAIL",
    "GL_ACCT_GRP",
    "GL_ACTUAL_DAY",
    "GL_ACTUAL_MONTH",
    "GL_ACCOUNT",
]

for table in tables:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"legacy.{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)
