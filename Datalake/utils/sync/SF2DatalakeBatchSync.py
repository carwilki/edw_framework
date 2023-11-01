import argparse
import json
from datetime import datetime, timedelta
from logging import INFO, getLogger

from pyspark.sql.session import SparkSession

from Datalake.utils.mergeUtils import mergeToSFv2
from Datalake.utils.sync.BatchManager import (
    BatchManager,
    BatchManagerException,
    BatchReaderSourceType,
    DateRangeBatchConfig,
)
from utils import parse_delta

parser = argparse.ArgumentParser()

parser.add_argument("env", type=str, help="Environment value")
parser.add_argument(, type=str, help="id of the batch job", required=True)
parser.add_argument("source_table", type=str, help="Source Table", required=True)
parser.add_argument("source_schema", type=str, help="Source Schema", required=True)
parser.add_argument(
    "target_schema", type=str, help="Target schema in the data lake", required=True
)
parser.add_argument(
    "target_table", type=str, help="Target table in the data lake", required=True
)
parser.add_argument(
    "source_type",
    type=str,
    help="the source type. either snowflake or netezza",
    required=True,
)
parser.add_argument(
    "source_filter",
    type=str,
    help="A fliter for the source table to limit the data",
    default=None,
)
parser.add_argument(
    "source_catalog",
    type=str,
    help="Optional source catalog for 3 level namespace",
    default=None,
)
parser.add_argument(
    "target_catalog",
    type=str,
    help="Optional target data lake catalog for 3 level namespace",
    default=None,
)
parser.add_argument(
    "keys", type=str, help="primary Keys for the delta table", required=True
)
parser.add_argument(
    "excluded_columns", type=str, help="exlcuded columns in the delta table", default=[]
)
parser.add_argument(
    "date_columns",
    type=str,
    help="colunms that are dates to be used as the iterator to get new data",
    required=True,
)
parser.add_argument(
    "start_dt", type=datetime, help="the start date for the batch", required=True
)
parser.add_argument(
    "end_dt", type=datetime, help="the end date for the batch", required=True
)
parser.add_argument(
    "interval",
    type=str,
    help="the interval for the batch",
    default=None,
)

args = parser.parse_args()
batchConfig: DateRangeBatchConfig = DateRangeBatchConfig()
batchConfig.env = args.env
batchConfig.batch_id = args.batch_id
batchConfig.source_table = args.source_table
batchConfig.source_type = BatchReaderSourceType(args.source_type)
batchConfig.source_filter = args.source_filter
batchConfig.keys = json.dumps([key for key in args.keys.split(",")])
batchConfig.excluded_columns = json.dumps(
    [col for col in args.excluded_columns.split(",")]
)
batchConfig.date_columns = json.dumps([date for date in args.date_columns.split(",")])
batchConfig.start_dt = args.start_dt.strftime("%Y-%m-%")
batchConfig.end_dt = args.end_dt.strftime("%Y-%m-%")
batchConfig.current_dt = None

if args.interval is not None:
    batchConfig.interval = parse_delta(args.interval)
else:
    batchConfig.interval = timedelta(weeks=1)
spark: SparkSession = SparkSession.getActiveSession()
logger = getLogger()
logger.setLevel(INFO)

try:
    print("SF2DatalakeBatchSync::starting")
    manager = BatchManager(spark, batchConfig)
    manager.next()
    print("SF2DatalakeBatchSync::end")
except Exception as e:
    print("SF2DatalakeBatchSync::error")
    print(
        f"""
        ****************************************************************
        SF2DatalakeBatchSync::ERROR:
        {str(e)}
        ****************************************************************
        """
    )

    raise e
