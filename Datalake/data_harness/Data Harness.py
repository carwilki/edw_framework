# Databricks notebook source
import os
import csv

# COMMAND ----------

from harness.config.EnvConfig import EnvConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.ValidatorConfig import ValidatorConfig
from harness.manager.HarnessJobManager import HarnessJobManager
from harness.sources.JDBCSourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig
from pyspark.sql.session import SparkSession

# COMMAND ----------

username = dbutils.secrets.get(scope="netezza_petsmart_keys", key="username")
password = dbutils.secrets.get(scope="netezza_petsmart_keys", key="password")

env = EnvConfig(
    workspace_url="https://3986616729757273.3.gcp.databricks.com/",
    workspace_token="dapi5492460db39d145778c9d436bbbf1842",
    metadata_schema="nzmigration",
    metadata_table="harness_metadata",
    snapshot_schema="nzmigration",
    snapshot_table_post_fix="_gold",
    jdbc_url="jdbc:netezza://172.16.73.181:5480/EDW_PRD",
    jdbc_user=username,
    jdbc_password=password,
    jdbc_driver="org.netezza.Driver",
)
job_id = "01298d4f-934f-439a-b80d-251987f5421"

# COMMAND ----------

with open(f"/Workspace/Repos/nz_migration/nz-databricks-migration/Datalake/data_harness/tableList_WMS_To_SCDS_Daily.csv","r") as file:
  tableList = csv.DictReader(file)
  sources = dict()
  for table in tableList:
     sc = JDBCSourceConfig(source_table = table['tables'], source_schema = table['source_schema'], source_filter = table['source_filter'])
     tc = TableTargetConfig(target_table = table['target_table'], target_schema="hive_metastore.nzmigration")
     snc = SnapshotConfig(job_id=job_id, target=tc, source=sc, name=table['tables'])
     sources[table['tables']] = snc
     hjc = HarnessJobConfig(job_id = job_id, sources = sources, inputs = {})


# COMMAND ----------

spark: SparkSession = SparkSession.getActiveSession()
hjm = HarnessJobManager(hjc, env, spark)
print("snapshotting")
hjm.snapshot()

# COMMAND ----------


