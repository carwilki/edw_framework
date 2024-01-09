from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from databricks.sdk import WorkspaceClient
from databricks.sdk.dbutils import FileInfo
from databricks.sdk.service.jobs import RunResultState
from datetime import datetime, timedelta
from pydantic import BaseModel
from Datalake.utils import secrets
from Datalake.utils.logger import getLogger
from Datalake.utils.parameters.ParameterData import ParameterData, ParameterFile


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

    def prep_path(self) -> str:
        """
        Returns the full prep path for the given env
        :return: a string with the address of the prep folder
        :rtype: str
        """
        return self.prep_folder + "/"

    def processing_path(self) -> str:
        """
        Returns the full processing path for the given env
        :return: a string with the address of the processing folder
        """
        return f"{self.prep_folder}processing/"

    def archive_path(self, date: datetime) -> str:
        """
        Returns the full archive path for the given env and date
        :param date: the date that for this file to be archived
        """
        return f"{self.archive_folder}{date.strftime('%Y%m%d')}/"

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
    the name of the workflow
    """
    parameter_file: str
    """
    the environment to run the job in dev,qa,prod
    """
    env: str
    """
    the timeout for the job
    """
    timeout: timedelta = timedelta(minutes=120)
    """
    the date format for the file name
    """
    datefmtstr: str = "%Y%m%d_%H%M%S"


class FileWorkflowController(object):
    """
    FileWorkflowController is a class that is used as an entry point for processing
    workflows that require files to be processed.

    The main objective is to manage the files and move them into the appropriate
    buckets for processing.This is done by creating a queue of files to be processed,
    that is scanned for any daily gaps in the expected file dates and processes the files
    in order of arriving date.
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
        self.logger = getLogger()
        self.session = spark
        self.timeout = config.timeout
        self.job_id = config.job_id
        self.env = config.env
        self.datefmtstr = config.datefmtstr
        self.parameter_file_name = config.parameter_file

        if self.job_id is None or len(self.job_id.strip()) == 0:
            raise ValueError("job_id must be set")

        if self.parameter_file_name is None:
            raise ValueError("there must be a parameter file for the workflow")

        if self.session is None:
            raise ValueError("spark must have a SparkSession instance")

        self.dbutils = DBUtils(spark=self.session)
        self._setup_workspace_api()
        self.parameter_file: ParameterFile = self._setup_parameter_file()
        self.file_configs = self._setup_file_configs()

    def execute(self):
        """
        runs the workflow
        """
        self._setup_processing_map()
        self._setup_processing_queue()
        self._validate_processing_queue()
        self._process_job_queue()

    def _setup_parameter_file(self) -> ParameterFile:
        """
        sets up the file configurations based on the parameter file
        :return: a dictionary of the file configurations keyed by the environment
        """
        pd = ParameterData(env=self.env, spark=self.session)
        return pd.get_parameter_file(self.parameter_file_name)

    def _setup_workspace_api(self):
        """
        sets up the job parameters needed to interact with the databricks workspace API
        """
        self.logger.info("setting up job params")
        # get the secrets needed to authenticate with the databricks workspace API
        token = secrets.get(scope="db-token-jobsapi", key="password")
        instance_id = secrets.get(scope="db-token-jobsapi", key="instance_id")
        # construct the URL to the databricks workspace API
        url = f"https://{instance_id}"
        # create a workspace client using the URL and token
        self.ws_client = WorkspaceClient(host=url, token=token)
        # log the URL and token to the console
        self.logger.info(f"url:{url}")
        self.logger.info(f"token:{token}")
        self.logger.info(f"instance_id:{instance_id}")
        # log that the setup is complete
        self.logger.info("complete")

    def _setup_file_configs(self) -> list[FileConfig]:
        self.logger.info("setting up file configs")
        fc = []
        for s, a in self.parameter_file.get_source_buckets_archive_pairs():
            fc.append(
                FileConfig(prep_folder=s, archive_folder=a, datefmtstr=self.datefmtstr)
            )
        self.logger.info("complete")
        self.logger.info("Fileconfigs:")
        return fc

    def _setup_processing_map(self) -> dict[datetime, dict[FileConfig, FileInfo]]:
        """
        Creates a dictionary of dates and their corresponding file configurations and files that need to be
        processed.

        Returns:
            dict[datetime, dict[FileConfig, FileInfo]]: A dictionary of dates and their corresponding file
            configurations and files that need to be processed.
        """
        self.logger.info("creating Processing map")
        # create a dictionary date-> bucketconfig -> file of files that need to be processed for the date.
        pmap: dict[datetime, dict[FileConfig, FileInfo]] = {}
        # foreach bucket
        self.logger.debug(self.parameter_file)
        for s, a in self.parameter_file.get_source_buckets_archive_pairs():
            fc = FileConfig(prep_folder=s, archive_folder=a, datefmtstr=self.datefmtstr)
            # list the contents of the bucket
            files = self.dbutils.fs.ls(fc.prep_path())
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
        self.logger.info("creating processing queue")
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
        self.logger.info("processing job queue")
        for dt in self.queue:
            self.logger.info(f"processing date: {dt}")
            try:
                self._move_to_processing(dt)
                self._run_workflow()
                self._move_to_archive(dt)
            except Exception as e:
                # if there is an error move everthing back to source bucket
                self._move_to_prep(dt)
                self.logger.info(f"Error processing date: {dt}")
                self.logger.info(f"Error: {e}")
                raise e

    def _run_workflow(self) -> None:
        """
        Runs the workflow for the given job id.

        This method uses the Databricks workspace client to run the job with the given job id.
        If the job does not complete successfully, an exception is raised.

        Raises:
        Exception: If the job does not complete successfully.
        """
        self.logger.info("running workflow")
        r = self.ws_client.jobs.run_now_and_wait(
            job_id=self.job_id, timeout=self.timeout
        )
        if r.state.result_state != RunResultState.SUCCESS:
            raise Exception(
                f"FileWorkflowController::_run_workflow::Job with Id {self.job_id} completed other than successfull: {r.state}"
            )
        else:
            self.logger.info(
                f"Job with Id {self.job_id} completed successfully: {r.state}"
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
                    self.logger.info(
                        f"""moving file: {file.path} to processing Path:{fc.processing_path()}"""
                    )
                    self.dbutils.fs.mv(file.path, f"{fc.processing_path()}/{file.name}")
                except Exception as e:
                    self.logger.error(f"Error moving file: {file.path}")
                    self.logger.info(f"Error: {e}")
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
        fc: FileConfig
        for fc in self.file_configs:
            # get the file for the bucket
            file = file_map[fc]
            if file is not None:
                try:
                    self.logger.info(
                        f"""moving file: {fc.processing_path()}/{file.name} to raw Path:{fc.archive_path(dt)}"""
                    )
                    self.dbutils.fs.mv(
                        fc.processing_path() + file.name,
                        fc.archive_path(dt) + file.name,
                    )
                except Exception as e:
                    self.logger.info(f"Error moving file: {file.path}")
                    self.logger.info(f"Error: {e}")
                    raise e
            else:
                raise ValueError(
                    f"FileWorkflowController::_move_to_raw::no file config found for date:{dt}"
                )

    def _move_to_prep(self, dt: datetime) -> None:
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
        fc: FileConfig
        for fc in self.file_configs:
            # get the file for the bucket
            file = file_map[fc]
            if file is not None:
                try:
                    self.logger.info(
                        f"""moving file: {fc.processing_path()}/{file.name} to raw Path:{fc.archive_path(dt)}"""
                    )
                    self.dbutils.fs.mv(
                        fc.processing_path() + file.name,
                        fc.prep_path() + file.name,
                    )
                except Exception as e:
                    self.logger.info(f"Error moving file: {file.path}")
                    self.logger.info(f"Error: {e}")
                    raise e
            else:
                raise ValueError(
                    f"FileWorkflowController::_move_to_prep::no file config found for date:{dt}"
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
            self.logger.info("processing map:")
            for k, v in self.processing_map.items():
                self.logger.info(f"processing date: {k}")
                for b, f in v.items():
                    self.logger.info(f"bucket: {b}")
                    self.logger.info(f"file: {f}")
            self.logger.info("complete")
        else:
            self.logger.info("processing map is empty")

    def _pp_processing_queue(self) -> None:
        """
        prints the processing queue to the console
        """
        if self.queue is not None:
            self.logger.info("processing queue:")
            for dt in self.queue:
                self.logger.info(f"processing date: {dt}")

            self.logger.info("complete")
        else:
            self.logger.info("processing queue is None")
