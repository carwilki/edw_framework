
from Datalake.utils.rocky_script_automation.RockyAutomationUtils.py import *
from pyspark.sql.functions import *
legacy_tables = ['SAP_SKU_LINK_TYPE', 'DP_SITE_VEND_PROFILE', 'DP_SKU_LINK']
copy_hist_to_legacy(legacy_tables)
