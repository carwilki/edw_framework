# Databricks notebook source
from pyspark.sql.functions import col
import csv

tables = ["PS2_ACCRUED_LABOR_WK", "PS_PAYROLL_CALENDAR", "WFA_DEPARTMENT", "PS_PAYROLL_AREA"]
pii_tables=["EMPL_EARNINGS_WK","EMPLOYEE_PROFILE_WK"]

for table in tables:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"legacy.{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)

for table in pii_tables:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"empl_protected.legacy_{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)
