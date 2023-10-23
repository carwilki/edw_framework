from pyspark.sql.functions import col
import csv


tables = [
    "DP_FORECAST_WEEK_HIST",
    "DP_ACCURACY_DAY",
    "DP_PRODUCT_LOCATION_SETTINGS_HIST",
    "DP_DEMAND_DAY",
    "DP_ORDER_PROJECTION_DAY_HIST",
    "DP_ORDER_PROJECTION_WEEK_HIST",
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

