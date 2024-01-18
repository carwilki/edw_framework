"""
Provides a usable Job interface to manage file based jobs.
this wrapper directly calls the file workflow controller.
and is used as an entry point to the file jobs

args:
    env:        -e --env the environment to run the job in dev,qa,prod
    job id:     -id --job_id the id of the job to run as a file job
    timeout:    -to --timeout
                the timeout to wait for the called job to complete.
"""
import argparse
from datetime import timedelta
from logging import INFO, getLogger

from pyspark.sql.session import SparkSession
from Datalake.utils.files.FileWorkflowController import (
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
    "-pf", "--paramter_file", type=str, help="parameter file", default=None
)

parser.add_argument(
    "-to",
    "--timeout",
    type=lambda x: parse_delta(x),
    help="the interval for the batch",
    default=timedelta(hours=2),
)

args = parser.parse_args()
env = args.env
job_id = args.job_id
timeout = args.timeout
parameter_file = args.paramter_file
spark: SparkSession = SparkSession.getActiveSession()
logger = getLogger()
logger.setLevel(INFO)

config = FileWorkflowControllerConfg(
    job_id=job_id, env=env, parameter_file=parameter_file, timeout=timeout
)
controller = FileWorkflowController(spark=spark, config=config)

logger.info("starting file based workflow job")
controller.execute()

logger.info("workflwork job completed successfully")