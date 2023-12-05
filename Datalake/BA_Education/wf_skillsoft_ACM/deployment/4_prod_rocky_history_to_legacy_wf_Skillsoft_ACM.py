from pyspark.sql.functions import col
import csv


tables = [
    "SKILLSOFT_ACM_LEARNING_PROGRAM",
    "SKILLSOFT_ACM_ASSET_ACTIVITY",
    "EDU_CERT_SUMMARY_CONS",
    "EDU_CERT_DAILY_CONS",
    "EDU_RESULT_CONS",
    "EDU_ASSESSMENTS_CONS",
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