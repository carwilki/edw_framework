"""
Provides a usable job interface to  the SnowflakeCDCWriter. this wrapper can be 
called to push cdc data to Snowflake.

in order to use this wrapper the souce table MUST have the change
data feed enabled on the datalake table.

args:
    env: the environment to use
    dl_catalog: optional datalake catalog that contains the table
    dl_schema: required datalake schema that contains the table
    dl_table: the table that is the source for the cdc data
    sf_database: the snowflake database to write to
    sf_schema: the snowflake schema to write to
    sf_table: the snowflake table to write to
    primaryKeys: shared primary keys of the tables
    excludedCols: cols that have been excluded from cdc
"""
import argparse
from logging import INFO, getLogger

from pyspark.sql.session import SparkSession

from Datalake.utils.Snowflake.SnowflakeCDCWriter import SnowflakeCDCWriter

parser = argparse.ArgumentParser()
parser.add_argument("env", type=str, help="Environment value", default="dev")
parser.add_argument(
    "dl_catalog",
    type=str,
    help="optional catalog that contains the table",
    default=None,
)
parser.add_argument(
    "dl_schema", type=str, help="required schema that contains the table"
)
parser.add_argument(
    "dl_table", type=str, help="the table that contains the data for cdc"
)
parser.add_argument("sf_database", type=str, help="the snowflake database to write to")
parser.add_argument("sf_schema", type=str, help="the snowflake schema to write to")
parser.add_argument("sf_table", type=str, help="the snoflake table to write to")
parser.add_argument("primaryKeys", type=str, help="Primary Keys of the table")
parser.add_argument(
    "excludedCols", type=str, help="Cols that have been excluded from the cdc"
)

args = parser.parse_args()
env = args.env
dl_catalog = args.dl_catalog
dl_schema = args.dl_schema
dl_table = args.dl_table
sf_database = args.sf_database
sf_schema = args.sf_schema
sf_table = args.sf_table
primaryKeys = [pKey for pKey in args.primaryKeys.split(",")]
excludedCols = [excludedCol for excludedCol in args.excludedCols.split(",")]

spark: SparkSession = SparkSession.getActiveSession()
logger = getLogger()
logger.setLevel(INFO)
sfcdc = SnowflakeCDCWriter(
    env=env,
    spark=spark,
    dl_schema=dl_schema,
    dl_table=dl_table,
    sf_database=sf_database,
    sf_schema=sf_schema,
    sf_table=sf_table,
    primary_keys=primaryKeys,
    update_excl_columns=excludedCols,
)

logger.info(
    "performing CDC write to Snowflake: %s\n\
            from Datalake %s",
    f"{sf_database}.{sf_schema}.{sf_table}",
    f"{dl_catalog}.{dl_schema}.{dl_table}",
)
sfcdc.push_cdc()
logger.info(
    "Data write to SF completed for table: %s", f"{sf_database}.{sf_schema}.{sf_table}"
)
