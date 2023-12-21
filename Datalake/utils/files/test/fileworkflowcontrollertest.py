# Databricks notebook source
from Datalake.utils.files.FileWorkflowController import (
    FileWorkflowControllerConfg,
    FileWorkflowController,
    FileConfig,
)

# COMMAND ----------
bucket1 = FileConfig(
    prep_folder="cwilkins01/file_controller_test/prep/file_controller_test_3",
    archive_folder="cwilkins01/file_controller_test/archive/file_controller_test_3",
)

bucket2 = FileConfig(
    prep_folder="cwilkins01/file_controller_test/prep/file_controller_test_4",
    archive_folder="cwilkins01/file_controller_test/archive/file_controller_test_4",
)

bucket3 = FileConfig(
    prep_folder="cwilkins01/file_controller_test/prep/file_controller_test_5",
    archive_folder="cwilkins01/file_controller_test/archive/file_controller_test_5",
)

config = FileWorkflowControllerConfg(job_id="1", env="dev", file_configs=[bucket1,bucket2,bucket3])
controller = FileWorkflowController(spark=spark,config=config)
