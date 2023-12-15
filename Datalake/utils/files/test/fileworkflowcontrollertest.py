# Databricks notebook source
from Datalake.utils.files.FileWorkflowController import FileWorkflowController, BucketConfig

# COMMAND ----------
bucket = BucketConfig(prep_bucket='gs://petm-bdpl-dev-shared-p1-gcs-gbl/cwilkins01/file_controller_test/prep/file_controller_test/',
                      archive_bucket='petm-bdpl-dev-shared-p1-gcs-gbl/cwilkins01/file_controller_test/archive/file_controller_test',)
controller = FileWorkflowController(buckets=[bucket],job_id='1',spark=spark)