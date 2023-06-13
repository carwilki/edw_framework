#Code converted on 2023-06-12 20:30:41
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from dbruntime import dbutils

# COMMAND ----------

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in job variables
# read_infa_paramfile('', 'm_WM_Asn_Detail_Status_PRE') ProcessingUtils

# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='1/1/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')

# COMMAND ----------
# Processing node SQ_Shortcut_to_ASN_DETAIL_STATUS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_ASN_DETAIL_STATUS = spark.read.jdbc(os.environ.get('OR_DC_WMS_CONNECT_STRING'), f"""SELECT
ASN_DETAIL_STATUS.ASN_DETAIL_STATUS,
ASN_DETAIL_STATUS.DESCRIPTION
FROM ASN_DETAIL_STATUS""", 
properties={
'user': os.environ.get('OR_DC_WMS_LOGIN'),
'password': os.environ.get('OR_DC_WMS_PASSWORD'),
'driver': os.environ.get('ORACLE_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ASN_DETAIL_STATUS_temp = SQ_Shortcut_to_ASN_DETAIL_STATUS.toDF(*["SQ_Shortcut_to_ASN_DETAIL_STATUS___" + col for col in SQ_Shortcut_to_ASN_DETAIL_STATUS.columns])

EXPTRANS = SQ_Shortcut_to_ASN_DETAIL_STATUS_temp.selectExpr( \
	"SQ_Shortcut_to_ASN_DETAIL_STATUS___sys_row_id as sys_row_id", \
	"$$DC_NBR as DC_NBR_EXP", \
	"SQ_Shortcut_to_ASN_DETAIL_STATUS___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"SQ_Shortcut_to_ASN_DETAIL_STATUS___DESCRIPTION as DESCRIPTION", \
	"CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
)

# COMMAND ----------
# Processing node Shortcut_to_WM_ASN_DETAIL_STATUS_PRE, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_WM_ASN_DETAIL_STATUS_PRE = EXPTRANS.selectExpr( \
	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
	"CAST(ASN_DETAIL_STATUS AS BIGINT) as ASN_DETAIL_STATUS", \
	"CAST(DESCRIPTION AS VARCHAR) as DESCRIPTION", \
	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_WM_ASN_DETAIL_STATUS_PRE.write.saveAsTable('WM_ASN_DETAIL_STATUS_PRE', mode = 'append')