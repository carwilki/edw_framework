"""
Provides a usable job interface to push data out of the lakehouse.

args:
    env: the environment to use
    source_catalog: optional datalake catalog that contains the table
    souce_schema: required datalake schema that contains the table
    source_table: the table that is the source for the cdc data
    key_scope : the scope that has the keys
    jdbc_url_key : the key for the jdbc url secret
    jdbc_username_key : the key for the jdbc username secret
    jdbc_password_key : the key for the jdbc password secret
    jdbc_stage_table : the fqn of the table to write the stage data to
    jdbc_ingest_sproc : the name of the sproc that move the stage data to the target table
"""
import argparse
from Datalake.utils.logger import getLogger

from pyspark.sql.session import SparkSession

from Datalake.utils.sync.cdc.delta.DeltaCDCWriter import DeltaCDCWriter

parser = argparse.ArgumentParser()
parser.add_argument("env", type=str, help="Environment value", default="dev")
parser.add_argument(
    "source_catalog",
    type=str,
    help="optional catalog that contains the table",
    default=None,
)
parser.add_argument(
    "souce_schema", type=str, help="required schema that contains the table"
)
parser.add_argument(
    "source_table", type=str, help="the table that contains the data for cdc"
)

parser.add_argument(
    "target_catalog", type=str, help="the snowflake database to write to", default=None
)
parser.add_argument("target_schema", type=str, help="the snowflake schema to write to")
parser.add_argument("target_table", type=str, help="the snoflake table to write to")
parser.add_argument("primaryKeys", type=str, help="Primary Keys of the table")
parser.add_argument(
    "excludedCols", type=str, help="Cols that have been excluded from the cdc"
)

args = parser.parse_args()
env = args.env
source_catalog = args.source_catalog
source_schema = args.source_schema
source_table = args.source_table
key_scope = args.key_scope
jdbc_url_key = args.jdbc_url_key
jdbc_username_key = args.jdbc_username_key
jdbc_password_key = args.jdbc_password_key
jdbc_stage_table = args.jdbc_stage_table
jdbc_ingest_sproc = args.jdbc_ingest_sproc
primaryKeys = [pKey for pKey in args.primaryKeys.split(",")]
excludedCols = [excludedCol for excludedCol in args.excludedCols.split(",")]

spark: SparkSession = SparkSession.getActiveSession()
logger = getLogger()
cdc = DeltaCDCWriter()

logger.info(
    "performing write from Lakehouse: %s\n\
            to JDBC %s",
    f"{source_catalog}.{source_schema}.{source_table}",
    f"stage tabel: {jdbc_stage_table}, ingest sproc: {jdbc_ingest_sproc}",
)
cdc.push()
logger.info(
    "Data write from Lakehouse completed for table: %s",
    f"{source_catalog}.{source_schema}.{source_table}",
)
