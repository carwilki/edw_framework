from queue import Queue
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from databricks.sdk.dbutils import FileInfo
from datetime import datetime, timedelta
from pydantic import BaseModel
from Datalake.utils import secrets
from Datalake.utils.files.vars import prep, raw, processing, prep_mount, raw_mount


class FileConfig(BaseModel):
    prep_path: str
    archive_path: str
    datefmtstr: str = "yyyymmdd_hh24mmss"


class FileWorkflowController(object):
    """_summary_

    Args:
        object (_type_): _description_
    """

    def __init__(
        self,
        buckets: list[FileConfig],
        job_id: str,
        spark: SparkSession,
        timeout: timedelta | None = None,
    ):
        self.buckets = buckets
        self.session = spark
        self.timeout = timeout
        self.job_id = job_id
        if self.job_id is None or len(self.job_id.strip()) == 0:
            raise ValueError("job_id must be set")

        if self.buckets is None:
            raise ValueError("buckets must have at least a BucketConfig instance")

        if self.session is None:
            raise ValueError("spark must have a SparkSession instance")

        self.dbutils = DBUtils(spark=self.session)
        self._setup_job_params()
        # self._mount_buckets()

        self._setup_processing_map()
        self._setup_processing_queue()
        # self._process_job_queue()
        # self._run_workflow()
        # self._un_mount_buckets()
        print(self.queue)

    def _setup_job_params(self):
        print("FileWorkflowController::_setup_job_params::setting up job params")
        token = secrets.get(scope="db-token-jobsapi", key="password")
        instance_id = secrets.get(scope="db-token-jobsapi", key="instance_id")
        url = f"https://{instance_id}"
        print(f"FileWorkflowController::_setup_job_params::url:{url}")
        print(f"FileWorkflowController::_setup_job_params::token:{token}")
        print(f"FileWorkflowController::_setup_job_params::instance_id:{instance_id}")
        print("FileWorkflowController::_setup_job_params::complete")

    def _setup_processing_map(self) -> dict[datetime, dict[FileConfig, FileInfo]]:
        print("FileWorkflowController::_get_all_files::creating Processing map")
        # create a dictionary date-> bucketconfig -> file of files that need to be processed for the date.
        pmap: dict[datetime, dict[FileConfig, FileInfo]] = {}
        # foreach bucket
        for bucket_config in self.buckets:
            # list the contents of the bucket
            files = self.dbutils.fs.ls(bucket_config.prep_path)
            # foreach file in the bucket
            for f in files:
                # if the size is 0 then its a directory and should be skipped.
                if f.size != 0:
                    date = self._extract_date(f, bucket_config.datefmtstr)
                    if date not in pmap:
                        pmap[date] = {bucket_config: f}
                    else:
                        pmap[date] += {bucket_config: f}

        print(
            f"""FileWorkflowController::_get_all_files::Processing map created:
{pmap}"""
        )
        self.processing_map = pmap

    def _setup_processing_queue(self):
        print(
            "FileWorkflowController::_setup_processing_queue::creating processing queue"
        )
        self.queue: Queue = Queue(len(self.processing_map.keys))
        sorted_dates = sorted(self.processing_map.keys(), reverse=False)
        for dt in sorted_dates:
            self.queue.put(dt)

    def _process_job_queue(self):
        print("FileWorkflowController::_process_job_queue::processing job queue")
        while not self.queue.empty():
            dt = self.queue.get()
            print(f"FileWorkflowController::_process_job_queue::processing date: {dt}")
            self._setup_files(dt)
            try:
                self._run_workflow()
                self._move_to_raw()
            except Exception as e:
                print(
                    f"FileWorkflowController::_process_job_queue::Error processing date: {dt}"
                )
                print(f"FileWorkflowController::_process_job_queue::Error: {e}")

    def _setup_files(self, dt: datetime) -> None:
        self._move_to_processing(dt)

    def _run_workflow(self) -> None:
        r = self.client.jobs.run_now_and_wait(job_id=self.job_id, timeout=self.timeout)
        if r.state.state_message != "SUCCESS":
            raise Exception(
                f"FileWorkflowController::_run_workflow::Job with Id {self.job_id} completed other than successfull: {r.state}"
            )

    def _clean_up_files(self, dt: datetime, failed: bool) -> None:
        if failed is True:
            self._move_to_prep(dt)
        else:
            self._move_to_raw(dt)

    def _move_to_processing(self, dt: datetime) -> None:
        # get the bucket -> file map for processing
        file_map = self.processing_map[dt]
        for b in self.buckets:
            # get the file for the bucket
            files = file_map[b]
            for f in files:
                self.dbutils.fs.mv(f.path, f"{b.prep_path}/processing/")

    def _move_to_prep(self, dt: datetime) -> None:
        # get the bucket -> file map for processing
        file_map = self.processing_map[dt]
        for b in self.buckets:
            # get the file for the bucket
            f = file_map[b]
            if f is not None:
                # move the file to the processing directory
                try:
                    self.client.dbfs.move(f"{b.prep_path}/{processing}/{f.name}")
                except Exception as e:
                    print(
                        f"FileWorkflowController::_move_to_prep::Error moving file: {f.path}"
                    )
                    print(f"FileWorkflowController::_move_to_prep::Error: {e}")
                    raise e

    def _move_to_raw(self, dt: datetime) -> None:
        file_map = self.processing_map[dt]
        for b in self.buckets:
            # get the file for the bucket
            f = file_map[b]
            if f is not None:
                try:
                    self.client.dbfs.move(
                        f"{b.prep_path}/{processing}/{f.name}", f"{raw}/{f.name}"
                    )
                except Exception as e:
                    print(
                        f"FileWorkflowController::_move_to_raw::Error moving file: {f.path}"
                    )
                    print(f"FileWorkflowController::_move_to_raw::Error: {e}")
                    raise e

    def _extract_date(file: FileInfo, dtstrfmt: str) -> datetime:
        # gs://bucket/some/path/to/file_yyyymmdd_hh24mmss.txt
        file: str = file.path.split("/")[-1], dtstrfmt
        # file_yyyymmdd_hh24mmss.txt
        name: str = file[0].split(".")[0]
        # file_yyyymmdd_hh24mmss
        dt = name.split("_")[-2]
        # yyyymmdd
        dt = datetime.strptime(dt, dtstrfmt)
        return dt

    def _extract_file_locations(
        self, path: str | None, datefmtstr: str | None
    ) -> dict[datetime, list[str]]:
        dbfs = self.client.dbfs
        if path is None:
            raise ValueError("path must be set")
        files = dbfs.list(path, recursive=True)
        date_dict: dict[datetime, list[str]] = {}
        for f in files:
            if f.is_dir is not True:
                dt = self._extract_date(f.path, datefmtstr)
                if dt not in date_dict.keys():
                    date_dict[dt] = [f.path]
                else:
                    date_dict[dt].append(f.path)

    def _mount_buckets(self):
        print("FileWorkflowController::_mount_buckets::mounting buckets")
        utils = self.dbutils
        utils.fs.mount(
            prep,
            prep_mount,
        )
        utils.fs.mount(raw, raw_mount)
        print("FileWorkflowController::_mount_buckets::complete")

    def _un_mount_buckets(self):
        print("FileWorkflowController::_un_mount_buckets::un-mounting buckets")
        utils = self.dbutils
        utils.fs.unmount(prep_mount)
        utils.fs.unmount(raw_mount)
        print("FileWorkflowController::_un_mount_buckets::complete")
