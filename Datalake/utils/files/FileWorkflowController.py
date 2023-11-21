from queue import Queue
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from databricks.sdk.dbutils import FileInfo
from datetime import datetime, timedelta
from pydantic import BaseModel
from Datalake.utils import secrets
from Datalake.utils.files.vars import prep, raw, processing, prep_mount, raw_mount


class BucketConfig(BaseModel):
    directory: str
    datefmtstr: str


class FileWorkflowController(object):
    def __init__(
        self,
        buckets: list[BucketConfig],
        job_id: str,
        spark: SparkSession,
        timeout: timedelta | None = None,
    ):
        self.buckets = buckets
        self.session = spark
        self.timeout = timeout
        self.job_id = job_id
        if self.job_id is None or len(self.job_id.strip) == 0:
            raise ValueError("job_id must be set")

        if self.buckets is None:
            raise ValueError("buckets must have at least a BucketConfig instance")

        if self.session is None:
            raise ValueError("spark must have a SparkSession instance")

        self.dbutils = DBUtils(self.session)

        self._mount_buckets()
        self._setup_job_params()
        self._setup_processing_map()
        self._setup_for_run()
        self._run_workflow()
        self._un_mount_buckets()

    def _setup_job_params(self):
        print("FileWorkflowController::_setup_job_params::setting up job params")
        token = secrets.get(scope="db-token-jobsapi", key="password")
        instance_id = secrets.get(scope="db-token-jobsapi", key="instance_id")
        url = f"https://{instance_id}"
        self.client = WorkspaceClient(host=url, token=token)
        print(f"FileWorkflowController::_setup_job_params::url:{url}")
        print(f"FileWorkflowController::_setup_job_params::token:{token}")
        print(f"FileWorkflowController::_setup_job_params::instance_id:{instance_id}")
        print(f"FileWorkflowController::_setup_job_params::complete")

    def _setup_processing_map(self) -> dict[datetime, list[str]]:
        print("FileWorkflowController::_get_all_files::creating Processing map")
        # create a dictionary date-> bucketconfig -> file of files that need to be processed for the date.
        date_dict: dict[datetime, dict[BucketConfig, FileInfo]] = {}
        # foreach bucket
        for bucket_config in self.buckets:
            # list the contents of the bucket
            r = self.client.dbfs.list(f"{prep}/{bucket_config.directory}")
            # foreach file in the bucket
            for f in r:
                # if the file is a directory, extract the date from the directory name
                if f.is_dir is True:
                    # extract the date from the directory name
                    dt = self._extract_date(f.path, bucket_config.datefmtstr)
                    # if the date is not already in the dictionary, add it
                    if dt not in date_dict.keys():
                        date_dict[dt] = {bucket_config: f}
                    # else if the date is already in the dictionary, add the file to the list of files to process
                    else:
                        date_dict[dt][bucket_config] = f

        print(
            f"""FileWorkflowController::_get_all_files::Processing map created:
{date_dict}"""
        )
        self.processing_map = date_dict

    def _setup_processing_queue(self):
        print(
            "FileWorkflowController::_setup_processing_queue::creating processing queue"
        )
        self.queue: Queue = Queue(len(self.processing_map))
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
                failed = False
            except Exception as e:
                failed = True
                print(
                    f"FileWorkflowController::_process_job_queue::Error processing date: {dt}"
                )
                print(f"FileWorkflowController::_process_job_queue::Error: {e}")
            finally:
                self._clean_up_files(dt, failed)

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
            f = file_map[b]
            if f is not None:
                # move the file to the processing directory
                self.client.dbfs.move(f.path, f"{prep}/{processing}/{f.name}")
            else:
                # if there is nothing there put an empty file in the processing directory
                self._copy_empty_to_processing()

    def _move_to_prep(self, dt: datetime) -> None:
        # get the bucket -> file map for processing
        file_map = self.processing_map[dt]
        for b in self.buckets:
            # get the file for the bucket
            f = file_map[b]
            if f is not None:
                # move the file to the processing directory
                try:
                    self.client.dbfs.move(
                        f"{b.directory}/{processing}/{f.name}", f.path
                    )
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
                        f"{b.directory}/{processing}/{f.name}", f"{raw}/{f.name}"
                    )
                except Exception as e:
                    print(
                        f"FileWorkflowController::_move_to_raw::Error moving file: {f.path}"
                    )
                    print(f"FileWorkflowController::_move_to_raw::Error: {e}")
                    raise e

    def _extract_date(dir: str, dtstrfmt: str) -> datetime:
        dt = datetime.strptime(dir.split("/")[-1], dtstrfmt)
        return dt

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
