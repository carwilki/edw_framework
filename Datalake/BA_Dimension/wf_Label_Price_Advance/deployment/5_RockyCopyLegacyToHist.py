# Databricks notebook source
from pyspark.sql.functions import col
import csv

tables = ['LABEL_DAY_STORE_SKU','LABEL_SIZE','LABEL_TYPE','LABEL_POG_TYPE']

for table in tables:    
  print(table)
  rocky_table = f"refine.{table}_history"
  target_table = f"legacy.{table}"
  df = spark.sql(f"select * from {rocky_table}")
  df = df.drop(col("bd_create_dt_tm"),col("bd_update_dt_tm"),col("source_file_name"))
  df.write.insertInto(f"{target_table}",overwrite=True)
