# Databricks notebook source
from Datalake.utils.files.FileWorkflowController import (
    FileWorkflowControllerConfg,
    FileWorkflowController,
    FileConfig,
)

# COMMAND ----------
bucket1 = FileConfig(
    prep_folder="cwilkins01/file_controller_test/prep/file_controller_test_12",
    archive_folder="cwilkins01/file_controller_test/archive/file_controller_test_12",
)

config = FileWorkflowControllerConfg(job_id="513193653968317", env="dev", file_configs=[bucket1])
controller = FileWorkflowController(spark=spark,config=config)
