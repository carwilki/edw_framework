import argparse
from datetime import datetime, timedelta
from logging import INFO, getLogger

from pyspark.sql.session import SparkSession
from Datalake.utils.sync.utils import parse_delta

from Datalake.utils.sync.batch.BatchManager import BatchManager
from Datalake.utils.sync.batch.BatchReaderSourceType import BatchReaderSourceType
from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig

parser = argparse.ArgumentParser()

parser.add_argument("-e", "--env", type=str, help="Environment value", required=True)
parser.add_argument("-id", "--batch_id", type=str, help="id of the batch job", required=True)
parser.add_argument(
    "-st", "--source_table", type=str, help="Source Snowflake Table FQN", required=True
)
parser.add_argument(
    "-tt", "--target_table", type=str, help="Target Data Lake Table FQN", required=True
)
parser.add_argument(
    "-t",
    "--source_type",
    type=lambda x: BatchReaderSourceType[x.upper()],
    help="the source type. either snowflake or netezza",
    choices=[BatchReaderSourceType.SNOWFLAKE, BatchReaderSourceType.NETEZZA],
    required=True,
)
parser.add_argument(
    "-sf",
    "--source_filter",
    type=str,
    help="A fliter for the source table to limit the data",
    default=None,
)
parser.add_argument(
    "-k", "--keys", type=str, help="primary Keys for the delta table", required=True
)
parser.add_argument(
    "-ec",
    "--excluded_columns",
    type=str,
    help="exlcuded columns in the delta table",
    default=None,
)
parser.add_argument(
    "-dc",
    "--date_columns",
    type=str,
    help="colunms that are dates to be used as the iterator to get new data",
    required=True,
)
parser.add_argument(
    "-sd",
    "--start_dt",
    type=lambda x: datetime.strptime(x, "%Y-%m-%d"),
    help="the start date for the batch",
    required=True,
)
parser.add_argument(
    "-ed",
    "--end_dt",
    type=lambda x: datetime.strptime(x, "%Y-%m-%d"),
    help="the end date for the batch",
    required=True,
)
parser.add_argument(
    "-in",
    "--interval",
    type=lambda x: parse_delta(x),
    help="the interval for the batch",
    default=None,
)

args = parser.parse_args()
batchConfig: DateRangeBatchConfig = DateRangeBatchConfig.empty()
batchConfig.env = args.env
batchConfig.batch_id = args.batch_id
batchConfig.target_table_fqn = args.target_table
batchConfig.source_table_fqn = args.source_table
batchConfig.source_type = BatchReaderSourceType(args.source_type)
batchConfig.source_filter = args.source_filter
batchConfig.keys = [key for key in args.keys.split(",")]
batchConfig.excluded_columns = [col for col in args.excluded_columns.split(",")]
batchConfig.date_columns = [date for date in args.date_columns.split(",")]
batchConfig.start_dt = args.start_dt
batchConfig.end_dt = args
batchConfig.interval = args.interval
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
