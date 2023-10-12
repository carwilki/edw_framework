import json
from Datalake.utils.mergeUtils import mergeToSFv2
from pyspark.sql import SparkSession
from datetime import datetime

primaryKeys = "LOCATION_ID,WM_SHIPMENT_ID,WM_STOP_SEQ"
primaryKeys = [pKey for pKey in primaryKeys.split(",")]
conditionCols = "LOAD_TSTMP,UPDATE_TSTMP"
conditionCols = [conditionCol for conditionCol in conditionCols.split(",")]
primaryKeys = json.dumps(primaryKeys)
conditionCols = json.dumps(conditionCols)
spark: SparkSession = SparkSession.getActiveSession()

mergeToSFv2("qa", "WM_STOP", primaryKeys, conditionCols, datetime(2023, 9, 26))
mergeToSFv2("qa", "WM_STOP", primaryKeys, conditionCols, datetime(2023, 9, 18))
