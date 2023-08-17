from pyspark.sql.functions import col
import csv
with open("/Workspace/Repos/gcpdatajobs-shared@petsmart.com/nz-databricks-migration/Datalake/WMS/WMS_TO_SCDS_DAILY/deployment/PROD_wms_scds_daily_rocky_config.csv",
    newline="",
) as csvfile:
    reader = csv.reader(csvfile, delimiter=",", quotechar='"')
    for row in reader:
        table = row[4]
        print(table)
        rocky_table = f"refine.{table}_history"
        target_table = f"refine.{table}"
        df = spark.sql(f"select * from {rocky_table}")
        df = df.drop(col("bd_create_dt_tm"),col("bd_update_dt_tm"),col("source_file_name"))
        df.write.insertInto(f"{target_table}",overwrite=True)
