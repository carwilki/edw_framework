# Databricks notebook source
from Datalake.utils.files.FileWorkflowController import (
    FileWorkflowController,
    FileConfig,
)

# COMMAND ----------
bucket = FileConfig(
    prep_path="cwilkins01/file_controller_test/prep/file_controller_test/",
    archive_path="cwilkins01/file_controller_test/archive/file_controller_test/",
)
controller = FileWorkflowController(buckets=[bucket], job_id="1", spark=spark)
