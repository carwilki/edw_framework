import json
from Datalake.utils.mergeUtils import mergeToSFv2
from pyspark.sql import SparkSession
from datetime import datetime

primaryKeys = "LOCATION_ID,WM_ITEM_PACKAGE_ID"
primaryKeys = [pKey for pKey in primaryKeys.split(",")]
conditionCols = "LOAD_TSTMP,UPDATE_TSTMP"
conditionCols = [conditionCol for conditionCol in conditionCols.split(",")]
primaryKeys = json.dumps(primaryKeys)
conditionCols = json.dumps(conditionCols)
spark: SparkSession = SparkSession.getActiveSession()

mergeToSFv2(
    "prod",
    "WM_ITEM_PACKAGE_CBO",
    primaryKeys,
    conditionCols,
    lb=datetime(2023, 8, 1),
    ub=datetime.now(),
)
