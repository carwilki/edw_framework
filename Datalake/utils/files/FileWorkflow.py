"""
Provides a usable Job interface to manage file based jobs.
this wrapper directly calls the file workflow controller.
and is used as an entry point to the file jobs

args:
    env:        -e --env the environment to run the job in dev,qa,prod
    job id:     -id --job_id the id of the job to run as a file job
    paths:      -p --paths the required paths as a tuple of
                strings (input_bucket,archive_bucket)
                passed in as a comma separated string of pairs
                "input_1::archive1,input_2::archive2"
    timeout:    -to --timeout
                the timeout to wait for the called job to complete.
"""
import argparse
from datetime import timedelta
from logging import INFO, getLogger

from pyspark.sql.session import SparkSession
from Datalake.utils.files.FileWorkflowController import (
    FileConfig,
    FileWorkflowController,
    FileWorkflowControllerConfg,
)
from Datalake.utils.TimeDeltaUtils import parse_delta

parser = argparse.ArgumentParser()
parser.add_argument(
    "-e", "--env", type=str, help="Environment value", default=None, required=True
)
parser.add_argument(
    "-id",
    "--job_id",
    type=str,
    help="the id of the job to run as a file job",
    default=None,
    required=True,
)
parser.add_argument(
    "-p",
    "--paths",
    type=str,
    help="""required
    gcs://prep_bucket/some/path/to/::gcs://archive_bucket/some/path/to/file_yyyymmdd_hh
    indicates the input path and the archive path for when processing is completed""",
    default=None,
)

parser.add_argument(
    "-to",
    "--timeout",
    type=lambda x: parse_delta(x),
    help="the interval for the batch",
    default=timedelta(hours=2),
)


def splitpaths(path: str) -> FileConfig:
    """
    Splits a path string in the form of "gcs://bucket/path/to/prep::gcs://bucket/path/to/archive"
    into two FileConfig objects.

    Args:
        path (str): The path string to split.

    Returns:
        FileConfig: A FileConfig object containing the input and output paths.

    Raises:
        ValueError: If the path is not in the expected format.
    """
    i, o = path.split("::")
    return FileConfig(prep_folder=i, archive_folder=o)


args = parser.parse_args()
env = args.env
paths = args.paths
job_id = args.job_id
timeout = args.timeout
pairs = paths.split(",")
file_configs = list(map(splitpaths, pairs))
spark: SparkSession = SparkSession.getActiveSession()
logger = getLogger()
logger.setLevel(INFO)

config = FileWorkflowControllerConfg(job_id=job_id, env=env, file_configs=file_configs)
controller = FileWorkflowController(spark=spark, config=config)

logger.info("starting file based workflow job")
controller.execute()

logger.info("workflwork job completed successfully")
