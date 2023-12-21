from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from databricks.sdk import WorkspaceClient
from databricks.sdk.dbutils import FileInfo
from datetime import datetime, timedelta
from pydantic import BaseModel
from Datalake.utils import secrets
from Datalake.utils.files import vars


class FileConfig(BaseModel):
    prep_folder: str
    archive_folder: str
    datefmtstr: str = "%Y%m%d_%H%M%S"
    max_time_gap = timedelta(days=1)

    def prep_path(self, env: str) -> str:
        return vars.getPrepBucket(env) + "/" + self.prep_folder + "/"

    def processing_path(self, env: str) -> str:
        return f"{vars.getPrepBucket(env)}/{self.prep_folder}/processing/"

    def archive_path(self, env: str, date: datetime) -> str:
        return f"{vars.getArchiveBucket(env)}/{self.archive_folder}/{date.strftime('%Y%m%d')}/"

    class Config:
        frozen = True


class FileWorkflowControllerConfg(BaseModel):
    job_id: str
    file_configs: list[FileConfig]
    env: str
    timeout: timedelta = timedelta(minutes=120)


class FileWorkflowController(object):
    """_summary_

    Args:
        object (_type_): _description_
    """

    def __init__(self, spark: SparkSession, config: FileWorkflowControllerConfg):
        self.file_configs = config.file_configs
        self.session = spark
        self.timeout = config.timeout
        self.job_id = config.job_id
        self.env = config.env
        self.max_time_gap = config.max_time_gap

        if self.job_id is None or len(self.job_id.strip()) == 0:
            raise ValueError("job_id must be set")

        if self.file_configs is None:
            raise ValueError("buckets must have at least a BucketConfig instance")

        if self.session is None:
            raise ValueError("spark must have a SparkSession instance")

        self.dbutils = DBUtils(spark=self.session)
        self._setup_job_params()
        # self._mount_buckets()
        self._setup_processing_map()
        self._setup_processing_queue()
        self._validate_processing_queue()
        self._process_job_queue()
        # self._run_workflow()

    def _setup_job_params(self):
        print("FileWorkflowController::_setup_job_params::setting up job params")
        token = secrets.get(scope="db-token-jobsapi", key="password")
        instance_id = secrets.get(scope="db-token-jobsapi", key="instance_id")
        url = f"https://{instance_id}"
        self.ws_client = WorkspaceClient(host=url, token=token)
        print(f"FileWorkflowController::_setup_job_params::url:{url}")
        print(f"FileWorkflowController::_setup_job_params::token:{token}")
        print(f"FileWorkflowController::_setup_job_params::instance_id:{instance_id}")
        print("FileWorkflowController::_setup_job_params::complete")

    def _setup_processing_map(self) -> dict[datetime, dict[FileConfig, FileInfo]]:
        print("FileWorkflowController::_get_all_files::creating Processing map")
        # create a dictionary date-> bucketconfig -> file of files that need to be processed for the date.
        pmap: dict[datetime, dict[FileConfig, FileInfo]] = {}
        # foreach bucket
        for fc in self.file_configs:
            # list the contents of the bucket
            files = self.dbutils.fs.ls(fc.prep_path(self.env))
            # foreach file in the bucket
            for f in files:
                # if the size is 0 then its a directory and should be skipped.
                if f.size != 0:
                    date = self._extract_date(f, fc.datefmtstr)

                    if date not in pmap:
                        pmap[date] = {fc: f}
                    else:
                        pmap[date][fc] = f
                        
        self.processing_map = pmap
        self._pp_file_date_map()
        self._pp_processing_map()

    def _setup_processing_queue(self):
        print(
            "FileWorkflowController::_setup_processing_queue::creating processing queue"
        )
        sorted_dates = sorted(self.processing_map.keys(), reverse=False)
        self.queue = sorted_dates
        self._pp_processing_queue()

    def _validate_processing_queue(self) -> None:
        for dt in self.queue:
            for fc in self.file_configs:
                file = self.processing_map[dt][fc]
                if file is None:
                    raise Exception(
                        f"FileWorkflowController::_validate_processing_queue::file is None for date: {dt} and config: {fc}"
                    )
            
    def _process_job_queue(self):
        print("FileWorkflowController::_process_job_queue::processing job queue")
        for dt in self.queue:
            dt = self.queue.get()
            print(f"FileWorkflowController::_process_job_queue::processing date: {dt}")

            try:
                self._move_to_processing(dt)
                # self._run_workflow()
                self._move_to_raw(dt)
            except Exception as e:
                print(
                    f"FileWorkflowController::_process_job_queue::Error processing date: {dt}"
                )
                print(f"FileWorkflowController::_process_job_queue::Error: {e}")
                raise e

    def _run_workflow(self) -> None:
        return None
        r = self.client.jobs.run_now_and_wait(job_id=self.job_id, timeout=self.timeout)
        if r.state.state_message != "SUCCESS":
            raise Exception(
                f"FileWorkflowController::_run_workflow::Job with Id {self.job_id} completed other than successfull: {r.state}"
            )

    def _move_to_processing(self, dt: datetime) -> None:
        # get the bucket -> file map for processing
        # Date -> FileConfig -> [FileInfo]
        file_map = self.processing_map[dt]
        # loop through the file configs for the given date
        for fc in self.file_configs:
            # for the date, for the config pull the file from it
            file = file_map[fc]
            if file is not None:
                try:
                    print(
                        f"""FileWorkflowController::_move_to_processing::moving file: {file.path}
                        \tto processing Path:{fc.processing_path(self.env)}"""
                    )
                    self.dbutils.fs.mv(
                        file.path, f"{fc.processing_path(self.env)}/{file.name}"
                    )
                except Exception as e:
                    print(
                        f"FileWorkflowController::_move_to_processing::Error moving file: {file.path}"
                    )
                    print(f"FileWorkflowController::_move_to_processing::Error: {e}")
                    raise e
            else:
                raise ValueError(
                    f"FileWorkflowController::_move_to_processing::no file config found for date:{dt}"
                )

    def _move_to_raw(self, dt: datetime) -> None:
        # get the bucket -> file map for processing
        # Date -> FileConfig -> [FileInfo]
        file_map = self.processing_map[dt]
        for fc in self.file_configs:
            # get the file for the bucket
            file = file_map[fc]
            if file is not None:
                try:
                    print(
                        f"""
                        FileWorkflowController::_move_to_raw::moving file: {fc.processing_path(self.env)}/{file.name}
                        \tto raw Path:{fc.archive_path(self.env,dt)}"""
                    )
                    self.dbutils.fs.mv(
                        fc.processing_path(self.env) + "/" + file.name,
                        fc.archive_path(self.env, dt) + "/" + file.name,
                    )
                except Exception as e:
                    print(
                        f"FileWorkflowController::_move_to_raw::Error moving file: {f.path}"
                    )
                    print(f"FileWorkflowController::_move_to_raw::Error: {e}")
                    raise e
            else:
                raise ValueError(
                    f"FileWorkflowController::_move_to_raw::no file config found for date:{dt}"
                )

    def _extract_date(self, file: FileInfo, dtstrfmt: str) -> datetime:
        # gs://bucket/some/path/to/file_yyyymmdd_hh24mmss.txt
        file: str = file.path.split("/")[-1], dtstrfmt
        # file_yyyymmdd_hh24mmss.txt
        name: str = file[0].split(".")[0]
        # file_yyyymmdd_hh24mmss
        # dt = name.split("_")[-2]
        ds = "_".join(s for s in name.split("_")[-2::])
        # yyyymmdd
        dt = datetime.strptime(ds, dtstrfmt)
        return dt

    def _pp_processing_map(self) -> None:
        if self.processing_map is not None:
            print("FileWorkflowController::_pp_processing_map::processing map:")
            for k, v in self.processing_map.items():
                print(
                    f"\tFileWorkflowController::_pp_processing_map::processing date: {k}"
                )
                for b, f in v.items():
                    print(
                        f"\t\tFileWorkflowController::_pp_processing_map::bucket: {b}"
                    )
                    print(f"\t\tFileWorkflowController::_pp_processing_map::file: {f}")
            print("FileWorkflowController::_pp_processing_map::complete")
        else:
            print(
                "\tFileWorkflowController::_pp_processing_map::processing map is empty"
            )

    def _pp_processing_queue(self) -> None:
        if self.queue is not None:
            print("FileWorkflowController::_pp_processing_queue::processing queue:")
            for d in self.queue:
                print(
                    f"\tFileWorkflowController::_pp_processing_queue::processing date: {dt}"
                )

            print("FileWorkflowController::_pp_processing_queue::complete")
        else:
            print(
                "\tFileWorkflowController::_pp_processing_queue::processing queue is None"
            )
