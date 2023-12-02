
from Datalake.utils.rocky_script_automation.RockyAutomationUtils.py import *
from pyspark.sql.functions import *

tables = ['SAP_SKU_LINK_TYPE']
trigger_rocky(tables)

