# Databricks notebook source
from pyspark.sql.functions import col
import csv

tables = ['POG_FLOOR_VERSION','FLOOR_FIXTURE','POG_FLOOR_FIXTURE_SECTION','FLOORPLAN_VERSION','POG_GROUP_INCLUSION_LIST','POG_GROUP_EXCLUSION_LIST','IKB_STATUS','POG_GROUP_VERSION']

for table in tables:    
  print(table)
  rocky_table = f"refine.{table}_history"
  target_table = f"legacy.{table}"
  df = spark.sql(f"select * from {rocky_table}")
  df = df.drop(col("bd_create_dt_tm"),col("bd_update_dt_tm"),col("source_file_name"))
  df.write.insertInto(f"{target_table}",overwrite=True)
