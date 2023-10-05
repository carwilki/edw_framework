# Databricks notebook source
from pyspark.sql.functions import col

tables = ["IC_CA_CLAIMS"]

PII_tables = ["IC_WC_CLAIMS"]

for table in tables:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"legacy.{table}"
    df = spark.sql(f"select * from {rocky_table}")
    df = df.drop(
        col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
    )
    df.write.insertInto(f"{target_table}", overwrite=True)


for table in PII_tables:
    print(table)
    rocky_table = f"refine.{table}_history"
    target_table = f"empl_protected.legacy_{table}"
    df = spark.sql(f"""select CLAIM_NBR
                        , a.DAY_DT
                        , a.LOCATION_ID
                        , a.SOURCE_TYPE_ID
                        , a.LINE_TYPE_CD
                        , a.ANIMAL_IND
                        , a.CLAIM_SUB_STATUS_CD
                        , a.INCURRED_EXP_AMT
                        , a.INCURRED_MEDICAL_AMT
                        , a.INCURRED_INDEM_LOSS_AMT
                        , a.INCURRED_OTHER_EXP_AMT
                        , a.INCURRED_TOTAL_AMT
                        , a.LOAD_DT
                        , a.UPDATE_DT
                        FROM 
                        (select *
                        , row_number() OVER (PARTITION BY CLAIM_NBR ORDER BY LOAD_DT desc) rnk 
                        from {rocky_table}) a
                        where a.rnk=1""")
#     df = df.drop(
#         col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
#     )
    df.write.insertInto(f"{target_table}", overwrite=True)
