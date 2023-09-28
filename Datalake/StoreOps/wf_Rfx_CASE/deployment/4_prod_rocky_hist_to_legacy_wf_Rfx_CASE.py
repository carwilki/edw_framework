# Databricks notebook source
from pyspark.sql.functions import col

tables = [
    "RFX_RTM_PROJECT",
    "RFX_RTM_TASK",
    "RFX_RTM_UNIT_ATTR",
    "RFX_RTM_PROJECT_MESSAGE",
    "RFX_RTM_PROJECT_RESPONSE",
    "RFX_RTM_LOOK_UP",
    "RFX_RTM_PROJECT_STATUS",
    "RFX_RTM_PROJECT_TYPE",
    "RFX_RTM_PROJECT_PRIORITY",
    "RFX_RTM_ROLE",
    "RFX_RTM_PROJECT_EXECUTION_STATUS",
	"RFX_RTM_UNIT_HIERARCHY"
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


