#Code converted on 2023-08-03 13:28:19
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
# COMMAND ----------

# Set parameter lookup values
#parameter_filename = 'TBD'
#parameter_section = 'TBD'
parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = pettraining_prd_sqlServer_training(env)

# COMMAND ----------
# Variable_declaration_comment
MAX_LOAD_DATE=genPrevRunDt('TRAINING_INVOICE', legacy, raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_Invoice, type SOURCE 
# COLUMN COUNT: 13

SQ_Shortcut_to_Invoice = jdbcSqlServerConnection(f"""(SELECT
Invoice.InvoiceId,
Invoice.Status,
Invoice.CountryAbbreviation,
Invoice.SKU,
Invoice.UPC,
Invoice.BasePrice,
Invoice.DiscountAmount,
Invoice.Subtotal,
cast (Invoice.CreateDateTime AT TIME ZONE 'US Mountain Standard Time' as DATETIME2) as CreateDateTime,
cast (Invoice.LastModifiedDateTime AT TIME ZONE 'US Mountain Standard Time' as DATETIME2) as LastModifiedDateTime,
Invoice.ClassPromotionId,
Invoice.PackagePromotionId,
Invoice.ReservationId
FROM Training.dbo.Invoice
WHERE LastModifiedDateTime > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Invoice_temp = SQ_Shortcut_to_Invoice.toDF(*["SQ_Shortcut_to_Invoice___" + col for col in SQ_Shortcut_to_Invoice.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_Invoice_temp.selectExpr( \
	"SQ_Shortcut_to_Invoice___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_Invoice___InvoiceId as InvoiceId", \
	"SQ_Shortcut_to_Invoice___Status as Status", \
	"SQ_Shortcut_to_Invoice___CountryAbbreviation as CountryAbbreviation", \
	"SQ_Shortcut_to_Invoice___SKU as SKU", \
	"SQ_Shortcut_to_Invoice___UPC as UPC", \
	"SQ_Shortcut_to_Invoice___BasePrice as BasePrice", \
	"SQ_Shortcut_to_Invoice___DiscountAmount as DiscountAmount", \
	"SQ_Shortcut_to_Invoice___Subtotal as Subtotal", \
	"SQ_Shortcut_to_Invoice___CreateDateTime as CreateDateTime", \
	"SQ_Shortcut_to_Invoice___LastModifiedDateTime as LastModifiedDateTime", \
	"SQ_Shortcut_to_Invoice___ClassPromotionId as ClassPromotionId", \
	"SQ_Shortcut_to_Invoice___PackagePromotionId as PackagePromotionId", \
	"SQ_Shortcut_to_Invoice___ReservationId as ReservationId", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)
# .withColumn("v_MAX_LOAD_DATE", SET {MAX_LOAD_DATE} = GREATEST({MAX_LOAD_DATE}, LastModifiedDateTime))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_INVOICE_PRE, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_TRAINING_INVOICE_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(InvoiceId AS INT) as INVOICE_ID", \
	"CAST(Status AS INT) as STATUS", \
	"CAST(CountryAbbreviation AS STRING) as COUNTRY_ABBREVIATION", \
	"CAST(SKU AS STRING) as SKU", \
	"CAST(UPC AS STRING) as UPC", \
	"CAST(BasePrice AS DECIMAL(19,4)) as BASE_PRICE", \
	"CAST(DiscountAmount AS DECIMAL(19,4)) as DISCOUNT_AMOUNT", \
	"CAST(Subtotal AS DECIMAL(19,4)) as SUB_TOTAL", \
	"CAST(CreateDateTime AS TIMESTAMP) as CREATE_DATE_TIME", \
	"CAST(LastModifiedDateTime AS TIMESTAMP) as LAST_MODIFIED_DATE_TIME", \
	"CAST(ClassPromotionId AS INT) as CLASS_PROMOTION_ID", \
	"CAST(PackagePromotionId AS INT) as PACKAGE_PROMOTION_ID", \
	"CAST(ReservationId AS INT) as RESERVATION_ID", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_INVOICE_PRE.write.saveAsTable(f'{raw}.TRAINING_INVOICE_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_INVOICE_PRE", "TRAINING_INVOICE_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_INVOICE_PRE", "TRAINING_INVOICE_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e

