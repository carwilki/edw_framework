# Databricks notebook source
from pyspark.sql.functions import col


pii_tables = ["BANFIELD_EMPLOYEE"]


for table in pii_tables:
    rocky_table = f"empl_sensitive.refine_{table}_history"
    target_table = f"empl_sensitive.refine_{table}"
    df = spark.sql(f"""select 
                    BANF_EMPL_ID, BANF_EMPL_FIRST_NAME, BANF_EMPL_LAST_NAME, BANF_EMPL_PETPERKS_ID, BANF_EMPL_STATUS_CD, BANF_EMPL_PIN, UPDATE_TSTMP, LOAD_TSTMP
                    FROM 
                    (select *
                    , row_number() OVER (PARTITION BY BANF_EMPL_ID ORDER BY LOAD_TSTMP desc) rnk 
                    from {rocky_table}) a
                    where a.rnk=1""")
#     df = df.drop(
#         col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
#     )
    df.write.insertInto(f"{target_table}", overwrite=True)
