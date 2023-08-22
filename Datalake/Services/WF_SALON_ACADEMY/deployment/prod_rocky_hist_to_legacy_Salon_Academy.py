# Databricks notebook source
from pyspark.sql.functions import col
import csv

tables = ['SALON_ACADEMY', 'SALON_ACADEMY_ASSESSMENT', 'SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER', 'SALON_ACADEMY_ASSESSMENT_DENY_REASON', 'SALON_ACADEMY_ASSESSMENT_FAIL_REASON', 'SALON_ACADEMY_ASSESSMENT_REASON', 'SALON_ACADEMY_ASSESSMENT_TRAINER', 'SALON_ACADEMY_TRAINING', 'SALON_ACADEMY_TRAVEL', 'SALON_ACADEMY_TYPE']

for table in tables:    
  print(table)
  rocky_table = f"refine.{table}_history"
  target_table = f"legacy.{table}"
  df = spark.sql(f"select * from {rocky_table}")
  df = df.drop(col("bd_create_dt_tm"),col("bd_update_dt_tm"),col("source_file_name"))
  df.write.insertInto(f"{target_table}",overwrite=True)
