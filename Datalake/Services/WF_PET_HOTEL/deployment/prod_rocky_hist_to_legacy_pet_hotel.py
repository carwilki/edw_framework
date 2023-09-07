# Databricks notebook source
from pyspark.sql.functions import col
import csv

tables = ['E_RES_ADD_ONS', 'E_RES_ADD_ON_CATEGORY', 'E_RES_SELECTED_ADD_ONS', 'E_RES_SELECTED_MEDICATIONS']
pii_tables = ['E_RES_REQUESTS','E_RES_PETS']

for table in tables:    
  print(table)
  rocky_table = f"refine.{table}_history"
  target_table = f"legacy.{table}"
  df = spark.sql(f"select * from {rocky_table}")
  df = df.drop(col("bd_create_dt_tm"),col("bd_update_dt_tm"),col("source_file_name"))
  df.write.insertInto(f"{target_table}",overwrite=True)
  
 
for table in pii_tables:    
  print(table)
  rocky_table = f"refine.{table}_history"
  target_table = f"cust_sensitive.legacy_{table}"
  df = spark.sql(f"select * from {rocky_table}")
  df = df.drop(col("bd_create_dt_tm"),col("bd_update_dt_tm"),col("source_file_name"))
  df.write.insertInto(f"{target_table}",overwrite=True) 