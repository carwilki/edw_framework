from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from databricks.sdk.dbutils import FileInfo
from datetime import timedelta
from pydantic import BaseModel
from Datalake.utils.files.vars import prep, raw, processing


class BucketConfig(BaseModel):
    bucket: str
    type: str


class FileWorkflowController(object):
    def __init__(
        self,
        buckets: list[BucketConfig],
        job_id: str,
        workspace_token: str,
        host: str,
        spark:SparkSession
    ):  
        self.session = spark
        self.dbutils = DBUtils(spark)
        self.client = WorkspaceClient(
            host=self.client.host,
            token=workspace_token,
        )
        self.buckets = buckets

        if self.buckets is None:
            raise ValueError("buckets must have at lease BucketConfig instance")

        # wrap in some fancy driver code
        self.client.jobs.run_now_and_wait(job_id=job_id, timeout=timedelta(hours=3))

    def build_file_dictionary_by_date(self):
        utils = self.dbutils
        for bucket_config in self.buckets:
            files : list[FileInfo] = utils.fs.ls(f"{bucket_config.bucket}/{prep}", recursive=True)
            for file in files:
                file_date = 
                
    def _extract_date(filename:str) -> str: