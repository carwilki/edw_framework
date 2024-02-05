"""
Provides a usable job interface to  the Lakhouse CDC Writer. this wrapper can be 
called to push cdc data to the Lakehouse.

in order to use this wrapper the souce table MUST have the change
data feed enabled on the datalake table.

args:
    env: the environment to use
    source_catalog: optional datalake catalog that contains the table
    souce_schema: required datalake schema that contains the table
    source_table: the table that is the source for the cdc data
    target_catalog: the target catalog to write to
    target_schema: the target schema to write to
    target_table: the target table to write to
    primaryKeys: shared primary keys of the tables
    excludedCols: cols that have been excluded from cdc
"""
import argparse
from logging import INFO, getLogger

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
target_catalog = args.target_catalog
target_schema = args.target_schema
target_table = args.target_table
primaryKeys = [pKey for pKey in args.primaryKeys.split(",")]
excludedCols = [excludedCol for excludedCol in args.excludedCols.split(",")]

spark: SparkSession = SparkSession.getActiveSession()
logger = getLogger()
cdc = DeltaCDCWriter()

logger.info(
    "performing CDC write to Lakehouse: %s\n\
            from Lakehosue %s",
    f"{target_schema}.{target_schema}.{target_table}",
    f"{source_catalog}.{source_schema}.{source_table}",
)
cdc.push()
logger.info(
    "Data write to Lakehouse completed for table: %s",
    f"{target_schema}.{target_schema}.{target_table}",
)
