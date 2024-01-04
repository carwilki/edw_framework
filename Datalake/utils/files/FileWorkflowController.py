from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from databricks.sdk import WorkspaceClient
from databricks.sdk.dbutils import FileInfo
from databricks.sdk.service.jobs import RunResultState
from datetime import datetime, timedelta
from pydantic import BaseModel
from Datalake.utils import secrets
from Datalake.utils.files import vars


class FileConfig(BaseModel):
    """
    File configs are models that map the details needed to
    process the file
    This inherits from Pydantic BaseModel
    """

    """
    the prep folder for processing
    """
    prep_folder: str
    """
    the archive folder for processing
    """
    archive_folder: str
    """
    the date format for the file name
    """
    datefmtstr: str = "%Y%m%d_%H%M%S"

    def prep_path(self, env: str) -> str:
        """
        Returns the full prep path for the given env
        :param env: the current evn
        :type env: str
        :return: a string with the address of the prep folder
        :rtype: str
        """
        return vars.getPrepBucket(env) + "/" + self.prep_folder + "/"

    def processing_path(self, env: str) -> str:
        """
        Returns the full processing path for the given env
        :param env: the current evn
        :type env: str
        :return: a string with the address of the processing folder
        """
        return f"{vars.getPrepBucket(env)}/{self.prep_folder}/processing/"

    def archive_path(self, env: str, date: datetime) -> str:
        """
        Returns the full archive path for the given env and date
        :param env: the current evn
        :type env: str
        :param date: the date that for this file to be archived
        """
        return f"{vars.getArchiveBucket(env)}/{self.archive_folder}/{date.strftime('%Y%m%d')}/"

    class Config:
        """
        freezes the config
        """

        frozen = True


class FileWorkflowControllerConfg(BaseModel):
    """
    FileWorkflowControllerConfg is a model that contains
    all of the config details to run a file based workflow job
    This inherits from Pydantic BaseModel
    """

    """
    the id of the job
    """
    job_id: str
    """
    the individual configs for each gcs://bucket/folder pair
    """
    file_configs: list[FileConfig]
    """
    the environment to run the job in dev,qa,prod
    """
    env: str
    """
    the timeout for the job
    """
    timeout: timedelta = timedelta(minutes=120)


class FileWorkflowController(object):
    """
    FileWorkflowController is a class that is used as an entry point for processing
    workflows that require files to be processed.

    The main objective is to manage the files and move them into the appropriate
    buckets for processing.This is done by creating a queue of files to be processed,
    that is scanned for any daily gaps in the expected file dates and processes the files
    in order of arriving date.

    Args:
        object (_type_): _description_
    """

    def __init__(self, spark: SparkSession, config: FileWorkflowControllerConfg):
        """
        sets up the FileWorkflowController object
        :param spark: needs a spark session
        :type spark: SparkSession
        :param config: a FileWorkflowControllerConfg object
        :type config: FileWorkflowControllerConfg
        :raises ValueError: fails if the job_id is not set
        :raises ValueError: fails if the buckets are not set
        :raises ValueError: fails if the spark session is not set
        """
        self.file_configs = config.file_configs
        self.session = spark
        self.timeout = config.timeout
        self.job_id = config.job_id
        self.env = config.env

        if self.job_id is None or len(self.job_id.strip()) == 0:
            raise ValueError("job_id must be set")

        if self.file_configs is None:
            raise ValueError("buckets must have at least a BucketConfig instance")

        if self.session is None:
            raise ValueError("spark must have a SparkSession instance")

        self.dbutils = DBUtils(spark=self.session)
        self._setup_job_params()

    def execute(self):
        """
        runs the workflow
        """
        
        self._setup_processing_map()
        self._setup_processing_queue()
        self._validate_processing_queue()
        self._process_job_queue()

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
        """
        Creates a dictionary of dates and their corresponding file configurations and files that need to be
        processed.

        Returns:
            dict[datetime, dict[FileConfig, FileInfo]]: A dictionary of dates and their corresponding file
            configurations and files that need to be processed.
        """
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
        self._pp_processing_map()

    def _setup_processing_queue(self):
        """
        Creates a queue of dates that need to be processed.

        This method sorts the dates in the processing map in reverse chronological order and sets the queue to
        the sorted dates.

        This method prints the processing queue to the console for debugging purposes.
        """
        print(
            "FileWorkflowController::_setup_processing_queue::creating processing queue"
        )
        sorted_dates = sorted(self.processing_map.keys(), reverse=False)
        self.queue = sorted_dates
        self._pp_processing_queue()

    def _validate_processing_queue(self) -> None:
        """
        Validates the processing queue by ensuring that all the required files are present in the processing map.

        Raises:
            Exception: If any of the required files are missing from the processing map.
        """
        # foreach date in the queue
        for dt in self.queue:
            # foreach file config in the queue
            for fc in self.file_configs:
                # if the file is not present in the processing map then raise an exception
                file = self.processing_map[dt][fc]
                if file is None:
                    raise Exception(
                        f"FileWorkflowController::_validate_processing_queue::file is None for date: {dt} and config: {fc}"
                    )

    def _process_job_queue(self):
        """
        Runs the workflow for each date in the queue.

        This method moves the files to the processing bucket, runs the workflow, and then moves the files to the raw bucket. 
        If an error occurs during any of these steps, the method logs the error and raises an exception.
        """
        print("FileWorkflowController::_process_job_queue::processing job queue")
        for dt in self.queue:
            print(f"FileWorkflowController::_process_job_queue::processing date: {dt}")
            try:
                self._move_to_processing(dt)
                self._run_workflow()
                self._move_to_archive(dt)
            except Exception as e:
                print(
                    f"FileWorkflowController::_process_job_queue::Error processing date: {dt}"
                )
                print(f"FileWorkflowController::_process_job_queue::Error: {e}")
                raise e

    def _run_workflow(self) -> None:
        """
        Runs the workflow for the given job id.

        This method uses the Databricks workspace client to run the job with the given job id.
        If the job does not complete successfully, an exception is raised.

        Raises:
        Exception: If the job does not complete successfully.
        """
        print("FileWorkflowController::_run_workflow::running workflow")
        r = self.ws_client.jobs.run_now_and_wait(
            job_id=self.job_id, timeout=self.timeout
        )
        if r.state.result_state != RunResultState.SUCCESS:
            raise Exception(
                f"FileWorkflowController::_run_workflow::Job with Id {self.job_id} completed other than successfull: {r.state}"
            )
        else:
            print(
                f"FileWorkflowController::_run_workflow::Job with Id {self.job_id} completed successfully: {r.state}"
            )

    def _move_to_processing(self, dt: datetime) -> None:
        """
        Moves the files for the given date to the processing bucket.

        Args:
            dt (datetime): The date for which the files need to be moved.

        Raises:
            ValueError: If no file config is found for the given date.
            Exception: If an error occurs while moving the files.
        """
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

    def _move_to_archive(self, dt: datetime) -> None:
        """
        Moves the files for the given date to the archive bucket.

        Args:
            dt (datetime): The date for which the files need to be moved.

        Raises:
            ValueError: If no file config is found for the given date.
            Exception: If an error occurs while moving the files.
        """
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
        """
        Extracts the date from the given file info and datetime format string.

        Args:
            file (FileInfo): The file info containing the path to the file.
            dtstrfmt (str): The datetime format string used to extract the date from the file name.

        Returns:
            datetime: The date extracted from the file name.

        """
        # gs://bucket/some/path/to/file_yyyymmdd_hh24mmss.txt
        file_name: str = file.path.split("/")[-1]
        # file_yyyymmdd_hh24mmss.txt
        name: str = file_name.split(".")[0]
        # file_yyyymmdd_hh24mmss
        # dt = name.split("_")[-2]
        ds: str = "_".join(s for s in name.split("_")[-2::])
        # yyyymmdd
        dt: datetime = datetime.strptime(ds, dtstrfmt)
        return dt

    def _pp_processing_map(self) -> None:
        """
        prints the processing map to the console
        """
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
        """
        prints the processing queue to the console
        """
        if self.queue is not None:
            print("FileWorkflowController::_pp_processing_queue::processing queue:")
            for dt in self.queue:
                print(
                    f"\tFileWorkflowController::_pp_processing_queue::processing date: {dt}"
                )

            print("FileWorkflowController::_pp_processing_queue::complete")
        else:
            print(
                "\tFileWorkflowController::_pp_processing_queue::processing queue is None"
            )
