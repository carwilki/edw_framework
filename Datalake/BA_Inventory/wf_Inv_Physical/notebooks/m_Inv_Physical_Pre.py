# Databricks notebook source
#Code converted on 2023-09-26 09:20:08
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

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Variable_declaration_comment
# SAPFiscalMonth=args.SAPFiscalMonth

# COMMAND ----------

# Processing node SQ_Shortcut_to_ZTB_RF_PHYINV_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_ZTB_RF_PHYINV_PRE = spark.sql(f"""SELECT
MANDT,
SITE,
ARTICLE,
COUNT_QTY,
POST_DATE
FROM {raw}.ZTB_RF_PHYINV_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Fil_ZTB, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ZTB_RF_PHYINV_PRE_temp = SQ_Shortcut_to_ZTB_RF_PHYINV_PRE.toDF(*["SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___" + col for col in SQ_Shortcut_to_ZTB_RF_PHYINV_PRE.columns])

Fil_ZTB = SQ_Shortcut_to_ZTB_RF_PHYINV_PRE_temp.selectExpr(
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___MANDT as MANDT",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___SITE as SITE",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___ARTICLE as ARTICLE",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___COUNT_QTY as COUNT_QTY",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___POST_DATE as POST_DATE").filter("POST_DATE IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SAP_IKPF_PRE, type SOURCE 
# COLUMN COUNT: 25

SQ_Shortcut_to_SAP_IKPF_PRE = spark.sql(f"""SELECT
MANDT,
DOC_NBR,
FISCAL_YR,
VGART,
SITE_NBR,
LGORT,
SOBKZ,
BLDAT,
GIDAT,
ZLDAT,
POSTING_DT,
MONAT,
USNAM,
SPERR,
ZSTAT,
DSTAT,
XBLNI,
LSTAT,
XBUFI,
KEORD,
ORDNG,
INVNU,
IBLTXT,
INVART,
WSTI_BSTAT
FROM {raw}.SAP_IKPF_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SAP_ISEG_PRE, type SOURCE 
# COLUMN COUNT: 65

SQ_Shortcut_to_SAP_ISEG_PRE = spark.sql(f"""SELECT
MANDT,
DOC_NBR,
FISCAL_YR,
ZEILI,
SKU_NBR,
SITE_NBR,
LGORT,
CHARG,
SOBKZ,
BSTAR,
KDAUF,
KDPOS,
KDEIN,
LIFNR,
KUNNR,
PLPLA,
USNAM,
AEDAT,
USNAZ,
ZLDAT,
USNAD,
BUDAT,
XBLNI,
XZAEL,
XDIFF,
XNZAE,
XLOEK,
XAMEI,
BOOK_QTY,
XNULL,
PHYSICAL_QTY,
MEINS,
ERFMG,
ERFME,
MBLNR,
MJAHR,
ZEILE,
NBLNR,
DIFF_AMT,
WAERS,
ABCIN,
PS_PSP_PNR,
VKWRT,
EXVKW,
BUCHW,
KWART,
VKWRA,
VKMZL,
VKNZL,
WRTZL,
WRTBM,
DIWZL,
ATTYP,
GRUND,
SAMAT,
XDISPATCH,
WSTI_COUNTDATE,
WSTI_COUNTTIME,
WSTI_FREEZEDATE,
WSTI_FREEZETIME,
WSTI_POSM,
WSTI_POSW,
WSTI_XCALC,
WSTI_ENTERDATE,
WSTI_ENTERTIME
FROM {raw}.SAP_ISEG_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Agg_ZTB, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

Agg_ZTB = Fil_ZTB.selectExpr(
	"MANDT as MANDT",
	"SITE as SITE",
	"ARTICLE as ARTICLE",
	"COUNT_QTY as i_COUNT_QTY",
	"POST_DATE as POST_DATE") \
	.groupBy("MANDT","SITE","ARTICLE","POST_DATE") \
	.agg( 
        sum(col('i_COUNT_QTY')).alias("COUNT_QTY")
	).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Fil_Book_Qty_Phys_Qty, type FILTER 
# COLUMN COUNT: 65

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_ISEG_PRE_temp = SQ_Shortcut_to_SAP_ISEG_PRE.toDF(*["SQ_Shortcut_to_SAP_ISEG_PRE___" + col for col in SQ_Shortcut_to_SAP_ISEG_PRE.columns])

Fil_Book_Qty_Phys_Qty = SQ_Shortcut_to_SAP_ISEG_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SAP_ISEG_PRE___MANDT as MANDT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___DOC_NBR as DOC_NBR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___FISCAL_YR as FISCAL_YR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___ZEILI as ZEILI",
	"SQ_Shortcut_to_SAP_ISEG_PRE___SKU_NBR as SKU_NBR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___SITE_NBR as SITE_NBR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___LGORT as LGORT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___CHARG as CHARG",
	"SQ_Shortcut_to_SAP_ISEG_PRE___SOBKZ as SOBKZ",
	"SQ_Shortcut_to_SAP_ISEG_PRE___BSTAR as BSTAR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___KDAUF as KDAUF",
	"SQ_Shortcut_to_SAP_ISEG_PRE___KDPOS as KDPOS",
	"SQ_Shortcut_to_SAP_ISEG_PRE___KDEIN as KDEIN",
	"SQ_Shortcut_to_SAP_ISEG_PRE___LIFNR as LIFNR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___KUNNR as KUNNR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___PLPLA as PLPLA",
	"SQ_Shortcut_to_SAP_ISEG_PRE___USNAM as USNAM",
	"SQ_Shortcut_to_SAP_ISEG_PRE___AEDAT as AEDAT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___USNAZ as USNAZ",
	"SQ_Shortcut_to_SAP_ISEG_PRE___ZLDAT as ZLDAT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___USNAD as USNAD",
	"SQ_Shortcut_to_SAP_ISEG_PRE___BUDAT as BUDAT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___XBLNI as XBLNI",
	"SQ_Shortcut_to_SAP_ISEG_PRE___XZAEL as XZAEL",
	"SQ_Shortcut_to_SAP_ISEG_PRE___XDIFF as XDIFF",
	"SQ_Shortcut_to_SAP_ISEG_PRE___XNZAE as XNZAE",
	"SQ_Shortcut_to_SAP_ISEG_PRE___XLOEK as XLOEK",
	"SQ_Shortcut_to_SAP_ISEG_PRE___XAMEI as XAMEI",
	"SQ_Shortcut_to_SAP_ISEG_PRE___BOOK_QTY as BOOK_QTY",
	"SQ_Shortcut_to_SAP_ISEG_PRE___XNULL as XNULL",
	"SQ_Shortcut_to_SAP_ISEG_PRE___PHYSICAL_QTY as PHYSICAL_QTY",
	"SQ_Shortcut_to_SAP_ISEG_PRE___MEINS as MEINS",
	"SQ_Shortcut_to_SAP_ISEG_PRE___ERFMG as ERFMG",
	"SQ_Shortcut_to_SAP_ISEG_PRE___ERFME as ERFME",
	"SQ_Shortcut_to_SAP_ISEG_PRE___MBLNR as MBLNR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___MJAHR as MJAHR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___ZEILE as ZEILE",
	"SQ_Shortcut_to_SAP_ISEG_PRE___NBLNR as NBLNR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___DIFF_AMT as DIFF_AMT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WAERS as WAERS",
	"SQ_Shortcut_to_SAP_ISEG_PRE___ABCIN as ABCIN",
	"SQ_Shortcut_to_SAP_ISEG_PRE___PS_PSP_PNR as PS_PSP_PNR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___VKWRT as VKWRT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___EXVKW as EXVKW",
	"SQ_Shortcut_to_SAP_ISEG_PRE___BUCHW as BUCHW",
	"SQ_Shortcut_to_SAP_ISEG_PRE___KWART as KWART",
	"SQ_Shortcut_to_SAP_ISEG_PRE___VKWRA as VKWRA",
	"SQ_Shortcut_to_SAP_ISEG_PRE___VKMZL as VKMZL",
	"SQ_Shortcut_to_SAP_ISEG_PRE___VKNZL as VKNZL",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WRTZL as WRTZL",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WRTBM as WRTBM",
	"SQ_Shortcut_to_SAP_ISEG_PRE___DIWZL as DIWZL",
	"SQ_Shortcut_to_SAP_ISEG_PRE___ATTYP as ATTYP",
	"SQ_Shortcut_to_SAP_ISEG_PRE___GRUND as GRUND",
	"SQ_Shortcut_to_SAP_ISEG_PRE___SAMAT as SAMAT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___XDISPATCH as XDISPATCH",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_COUNTDATE as WSTI_COUNTDATE",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_COUNTTIME as WSTI_COUNTTIME",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_FREEZEDATE as WSTI_FREEZEDATE",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_FREEZETIME as WSTI_FREEZETIME",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_POSM as WSTI_POSM",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_POSW as WSTI_POSW",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_XCALC as WSTI_XCALC",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_ENTERDATE as WSTI_ENTERDATE",
	"SQ_Shortcut_to_SAP_ISEG_PRE___WSTI_ENTERTIME as WSTI_ENTERTIME").filter("BOOK_QTY + PHYSICAL_QTY > 0").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Exp_ZTB, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
Agg_ZTB_temp = Agg_ZTB.toDF(*["Agg_ZTB___" + col for col in Agg_ZTB.columns])

Exp_ZTB = Agg_ZTB_temp.selectExpr(
	"Agg_ZTB___sys_row_id as sys_row_id",
	"Agg_ZTB___MANDT as MANDT",
	"Agg_ZTB___SITE as SITE",
	"Agg_ZTB___ARTICLE as ARTICLE",
	"Agg_ZTB___COUNT_QTY as COUNT_QTY",
	"DATE_ADD(Agg_ZTB___POST_DATE, 1) as ZTB_POST_DATE"
)

# COMMAND ----------

# Processing node Jnr_ISEG_IKPF, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 90

# for each involved DataFrame, append the dataframe name to each column
Fil_Book_Qty_Phys_Qty_temp = Fil_Book_Qty_Phys_Qty.toDF(*["Fil_Book_Qty_Phys_Qty___" + col for col in Fil_Book_Qty_Phys_Qty.columns])
SQ_Shortcut_to_SAP_IKPF_PRE_temp = SQ_Shortcut_to_SAP_IKPF_PRE.toDF(*["SQ_Shortcut_to_SAP_IKPF_PRE___" + col for col in SQ_Shortcut_to_SAP_IKPF_PRE.columns])

Jnr_ISEG_IKPF = SQ_Shortcut_to_SAP_IKPF_PRE_temp.join(Fil_Book_Qty_Phys_Qty_temp,[SQ_Shortcut_to_SAP_IKPF_PRE_temp.SQ_Shortcut_to_SAP_IKPF_PRE___MANDT == Fil_Book_Qty_Phys_Qty_temp.Fil_Book_Qty_Phys_Qty___MANDT, SQ_Shortcut_to_SAP_IKPF_PRE_temp.SQ_Shortcut_to_SAP_IKPF_PRE___DOC_NBR == Fil_Book_Qty_Phys_Qty_temp.Fil_Book_Qty_Phys_Qty___DOC_NBR, SQ_Shortcut_to_SAP_IKPF_PRE_temp.SQ_Shortcut_to_SAP_IKPF_PRE___FISCAL_YR == Fil_Book_Qty_Phys_Qty_temp.Fil_Book_Qty_Phys_Qty___FISCAL_YR],'inner').selectExpr(
	"Fil_Book_Qty_Phys_Qty___MANDT as MANDT",
	"Fil_Book_Qty_Phys_Qty___DOC_NBR as DOC_NBR",
	"Fil_Book_Qty_Phys_Qty___FISCAL_YR as FISCAL_YR",
	"Fil_Book_Qty_Phys_Qty___ZEILI as ZEILI",
	"Fil_Book_Qty_Phys_Qty___SKU_NBR as SKU_NBR",
	"Fil_Book_Qty_Phys_Qty___SITE_NBR as SITE_NBR",
	"Fil_Book_Qty_Phys_Qty___LGORT as LGORT",
	"Fil_Book_Qty_Phys_Qty___CHARG as CHARG",
	"Fil_Book_Qty_Phys_Qty___SOBKZ as SOBKZ",
	"Fil_Book_Qty_Phys_Qty___BSTAR as BSTAR",
	"Fil_Book_Qty_Phys_Qty___KDAUF as KDAUF",
	"Fil_Book_Qty_Phys_Qty___KDPOS as KDPOS",
	"Fil_Book_Qty_Phys_Qty___KDEIN as KDEIN",
	"Fil_Book_Qty_Phys_Qty___LIFNR as LIFNR",
	"Fil_Book_Qty_Phys_Qty___KUNNR as KUNNR",
	"Fil_Book_Qty_Phys_Qty___PLPLA as PLPLA",
	"Fil_Book_Qty_Phys_Qty___USNAM as USNAM",
	"Fil_Book_Qty_Phys_Qty___AEDAT as AEDAT",
	"Fil_Book_Qty_Phys_Qty___USNAZ as USNAZ",
	"Fil_Book_Qty_Phys_Qty___ZLDAT as ZLDAT",
	"Fil_Book_Qty_Phys_Qty___USNAD as USNAD",
	"Fil_Book_Qty_Phys_Qty___BUDAT as BUDAT",
	"Fil_Book_Qty_Phys_Qty___XBLNI as XBLNI",
	"Fil_Book_Qty_Phys_Qty___XZAEL as XZAEL",
	"Fil_Book_Qty_Phys_Qty___XDIFF as XDIFF",
	"Fil_Book_Qty_Phys_Qty___XNZAE as XNZAE",
	"Fil_Book_Qty_Phys_Qty___XLOEK as XLOEK",
	"Fil_Book_Qty_Phys_Qty___XAMEI as XAMEI",
	"Fil_Book_Qty_Phys_Qty___BOOK_QTY as BOOK_QTY",
	"Fil_Book_Qty_Phys_Qty___XNULL as XNULL",
	"Fil_Book_Qty_Phys_Qty___PHYSICAL_QTY as PHYSICAL_QTY",
	"Fil_Book_Qty_Phys_Qty___MEINS as MEINS",
	"Fil_Book_Qty_Phys_Qty___ERFMG as ERFMG",
	"Fil_Book_Qty_Phys_Qty___ERFME as ERFME",
	"Fil_Book_Qty_Phys_Qty___MBLNR as MBLNR",
	"Fil_Book_Qty_Phys_Qty___MJAHR as MJAHR",
	"Fil_Book_Qty_Phys_Qty___ZEILE as ZEILE",
	"Fil_Book_Qty_Phys_Qty___NBLNR as NBLNR",
	"Fil_Book_Qty_Phys_Qty___DIFF_AMT as DIFF_AMT",
	"Fil_Book_Qty_Phys_Qty___WAERS as WAERS",
	"Fil_Book_Qty_Phys_Qty___ABCIN as ABCIN",
	"Fil_Book_Qty_Phys_Qty___PS_PSP_PNR as PS_PSP_PNR",
	"Fil_Book_Qty_Phys_Qty___VKWRT as VKWRT",
	"Fil_Book_Qty_Phys_Qty___EXVKW as EXVKW",
	"Fil_Book_Qty_Phys_Qty___BUCHW as BUCHW",
	"Fil_Book_Qty_Phys_Qty___KWART as KWART",
	"Fil_Book_Qty_Phys_Qty___VKWRA as VKWRA",
	"Fil_Book_Qty_Phys_Qty___VKMZL as VKMZL",
	"Fil_Book_Qty_Phys_Qty___VKNZL as VKNZL",
	"Fil_Book_Qty_Phys_Qty___WRTZL as WRTZL",
	"Fil_Book_Qty_Phys_Qty___WRTBM as WRTBM",
	"Fil_Book_Qty_Phys_Qty___DIWZL as DIWZL",
	"Fil_Book_Qty_Phys_Qty___ATTYP as ATTYP",
	"Fil_Book_Qty_Phys_Qty___GRUND as GRUND",
	"Fil_Book_Qty_Phys_Qty___SAMAT as SAMAT",
	"Fil_Book_Qty_Phys_Qty___XDISPATCH as XDISPATCH",
	"Fil_Book_Qty_Phys_Qty___WSTI_COUNTDATE as WSTI_COUNTDATE",
	"Fil_Book_Qty_Phys_Qty___WSTI_COUNTTIME as WSTI_COUNTTIME",
	"Fil_Book_Qty_Phys_Qty___WSTI_FREEZEDATE as WSTI_FREEZEDATE",
	"Fil_Book_Qty_Phys_Qty___WSTI_FREEZETIME as WSTI_FREEZETIME",
	"Fil_Book_Qty_Phys_Qty___WSTI_POSM as WSTI_POSM",
	"Fil_Book_Qty_Phys_Qty___WSTI_POSW as WSTI_POSW",
	"Fil_Book_Qty_Phys_Qty___WSTI_XCALC as WSTI_XCALC",
	"Fil_Book_Qty_Phys_Qty___WSTI_ENTERDATE as WSTI_ENTERDATE",
	"Fil_Book_Qty_Phys_Qty___WSTI_ENTERTIME as WSTI_ENTERTIME",
	"SQ_Shortcut_to_SAP_IKPF_PRE___MANDT as MANDT1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___DOC_NBR as DOC_NBR1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___FISCAL_YR as FISCAL_YR1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___VGART as VGART",
	"SQ_Shortcut_to_SAP_IKPF_PRE___SITE_NBR as SITE_NBR1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___LGORT as LGORT1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___SOBKZ as SOBKZ1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___BLDAT as BLDAT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___GIDAT as GIDAT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___ZLDAT as ZLDAT1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___POSTING_DT as POSTING_DT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___MONAT as MONAT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___USNAM as USNAM1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___SPERR as SPERR",
	"SQ_Shortcut_to_SAP_IKPF_PRE___ZSTAT as ZSTAT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___DSTAT as DSTAT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___XBLNI as XBLNI1",
	"SQ_Shortcut_to_SAP_IKPF_PRE___LSTAT as LSTAT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___XBUFI as XBUFI",
	"SQ_Shortcut_to_SAP_IKPF_PRE___KEORD as KEORD",
	"SQ_Shortcut_to_SAP_IKPF_PRE___ORDNG as ORDNG",
	"SQ_Shortcut_to_SAP_IKPF_PRE___INVNU as INVNU",
	"SQ_Shortcut_to_SAP_IKPF_PRE___IBLTXT as IBLTXT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___INVART as INVART",
	"SQ_Shortcut_to_SAP_IKPF_PRE___WSTI_BSTAT as WSTI_BSTAT")

# COMMAND ----------

# Processing node Jnr_ZTB, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 96

# for each involved DataFrame, append the dataframe name to each column
Exp_ZTB_temp = Exp_ZTB.toDF(*["Exp_ZTB___" + col for col in Exp_ZTB.columns])
Jnr_ISEG_IKPF_temp = Jnr_ISEG_IKPF.toDF(*["Jnr_ISEG_IKPF___" + col for col in Jnr_ISEG_IKPF.columns])

Jnr_ZTB = Exp_ZTB_temp.join(Jnr_ISEG_IKPF_temp,[Exp_ZTB_temp.Exp_ZTB___MANDT == Jnr_ISEG_IKPF_temp.Jnr_ISEG_IKPF___MANDT, Exp_ZTB_temp.Exp_ZTB___SITE == Jnr_ISEG_IKPF_temp.Jnr_ISEG_IKPF___SITE_NBR, Exp_ZTB_temp.Exp_ZTB___ARTICLE == Jnr_ISEG_IKPF_temp.Jnr_ISEG_IKPF___SKU_NBR, Exp_ZTB_temp.Exp_ZTB___ZTB_POST_DATE == Jnr_ISEG_IKPF_temp.Jnr_ISEG_IKPF___GIDAT],'right_outer').selectExpr(
	"Jnr_ISEG_IKPF___MANDT as MANDT",
	"Jnr_ISEG_IKPF___DOC_NBR as DOC_NBR",
	"Jnr_ISEG_IKPF___FISCAL_YR as FISCAL_YR",
	"Jnr_ISEG_IKPF___ZEILI as ZEILI",
	"Jnr_ISEG_IKPF___SKU_NBR as SKU_NBR",
	"Jnr_ISEG_IKPF___SITE_NBR as SITE_NBR",
	"Jnr_ISEG_IKPF___LGORT as LGORT",
	"Jnr_ISEG_IKPF___CHARG as CHARG",
	"Jnr_ISEG_IKPF___SOBKZ as SOBKZ",
	"Jnr_ISEG_IKPF___BSTAR as BSTAR",
	"Jnr_ISEG_IKPF___KDAUF as KDAUF",
	"Jnr_ISEG_IKPF___KDPOS as KDPOS",
	"Jnr_ISEG_IKPF___KDEIN as KDEIN",
	"Jnr_ISEG_IKPF___LIFNR as LIFNR",
	"Jnr_ISEG_IKPF___KUNNR as KUNNR",
	"Jnr_ISEG_IKPF___PLPLA as PLPLA",
	"Jnr_ISEG_IKPF___USNAM as USNAM",
	"Jnr_ISEG_IKPF___AEDAT as AEDAT",
	"Jnr_ISEG_IKPF___USNAZ as USNAZ",
	"Jnr_ISEG_IKPF___ZLDAT as ZLDAT",
	"Jnr_ISEG_IKPF___USNAD as USNAD",
	"Jnr_ISEG_IKPF___BUDAT as BUDAT",
	"Jnr_ISEG_IKPF___XBLNI as XBLNI",
	"Jnr_ISEG_IKPF___XZAEL as XZAEL",
	"Jnr_ISEG_IKPF___XDIFF as XDIFF",
	"Jnr_ISEG_IKPF___XNZAE as XNZAE",
	"Jnr_ISEG_IKPF___XLOEK as XLOEK",
	"Jnr_ISEG_IKPF___XAMEI as XAMEI",
	"Jnr_ISEG_IKPF___BOOK_QTY as BOOK_QTY",
	"Jnr_ISEG_IKPF___XNULL as XNULL",
	"Jnr_ISEG_IKPF___PHYSICAL_QTY as PHYSICAL_QTY",
	"Jnr_ISEG_IKPF___MEINS as MEINS",
	"Jnr_ISEG_IKPF___ERFMG as ERFMG",
	"Jnr_ISEG_IKPF___ERFME as ERFME",
	"Jnr_ISEG_IKPF___MBLNR as MBLNR",
	"Jnr_ISEG_IKPF___MJAHR as MJAHR",
	"Jnr_ISEG_IKPF___ZEILE as ZEILE",
	"Jnr_ISEG_IKPF___NBLNR as NBLNR",
	"Jnr_ISEG_IKPF___DIFF_AMT as DIFF_AMT",
	"Jnr_ISEG_IKPF___WAERS as WAERS",
	"Jnr_ISEG_IKPF___ABCIN as ABCIN",
	"Jnr_ISEG_IKPF___PS_PSP_PNR as PS_PSP_PNR",
	"Jnr_ISEG_IKPF___VKWRT as VKWRT",
	"Jnr_ISEG_IKPF___EXVKW as EXVKW",
	"Jnr_ISEG_IKPF___BUCHW as BUCHW",
	"Jnr_ISEG_IKPF___KWART as KWART",
	"Jnr_ISEG_IKPF___VKWRA as VKWRA",
	"Jnr_ISEG_IKPF___VKMZL as VKMZL",
	"Jnr_ISEG_IKPF___VKNZL as VKNZL",
	"Jnr_ISEG_IKPF___WRTZL as WRTZL",
	"Jnr_ISEG_IKPF___WRTBM as WRTBM",
	"Jnr_ISEG_IKPF___DIWZL as DIWZL",
	"Jnr_ISEG_IKPF___ATTYP as ATTYP",
	"Jnr_ISEG_IKPF___GRUND as GRUND",
	"Jnr_ISEG_IKPF___SAMAT as SAMAT",
	"Jnr_ISEG_IKPF___XDISPATCH as XDISPATCH",
	"Jnr_ISEG_IKPF___WSTI_COUNTDATE as WSTI_COUNTDATE",
	"Jnr_ISEG_IKPF___WSTI_COUNTTIME as WSTI_COUNTTIME",
	"Jnr_ISEG_IKPF___WSTI_FREEZEDATE as WSTI_FREEZEDATE",
	"Jnr_ISEG_IKPF___WSTI_FREEZETIME as WSTI_FREEZETIME",
	"Jnr_ISEG_IKPF___WSTI_POSM as WSTI_POSM",
	"Jnr_ISEG_IKPF___WSTI_POSW as WSTI_POSW",
	"Jnr_ISEG_IKPF___WSTI_XCALC as WSTI_XCALC",
	"Jnr_ISEG_IKPF___WSTI_ENTERDATE as WSTI_ENTERDATE",
	"Jnr_ISEG_IKPF___WSTI_ENTERTIME as WSTI_ENTERTIME",
	"Jnr_ISEG_IKPF___MANDT1 as MANDT1",
	"Jnr_ISEG_IKPF___DOC_NBR1 as DOC_NBR1",
	"Jnr_ISEG_IKPF___FISCAL_YR1 as FISCAL_YR1",
	"Jnr_ISEG_IKPF___VGART as VGART",
	"Jnr_ISEG_IKPF___SITE_NBR1 as SITE_NBR1",
	"Jnr_ISEG_IKPF___LGORT1 as LGORT1",
	"Jnr_ISEG_IKPF___SOBKZ1 as SOBKZ1",
	"Jnr_ISEG_IKPF___BLDAT as BLDAT",
	"Jnr_ISEG_IKPF___GIDAT as GIDAT",
	"Jnr_ISEG_IKPF___ZLDAT1 as ZLDAT1",
	"Jnr_ISEG_IKPF___POSTING_DT as POSTING_DT",
	"Jnr_ISEG_IKPF___MONAT as MONAT",
	"Jnr_ISEG_IKPF___USNAM1 as USNAM1",
	"Jnr_ISEG_IKPF___SPERR as SPERR",
	"Jnr_ISEG_IKPF___ZSTAT as ZSTAT",
	"Jnr_ISEG_IKPF___DSTAT as DSTAT",
	"Jnr_ISEG_IKPF___XBLNI1 as XBLNI1",
	"Jnr_ISEG_IKPF___LSTAT as LSTAT",
	"Jnr_ISEG_IKPF___XBUFI as XBUFI",
	"Jnr_ISEG_IKPF___KEORD as KEORD",
	"Jnr_ISEG_IKPF___ORDNG as ORDNG",
	"Jnr_ISEG_IKPF___INVNU as INVNU",
	"Jnr_ISEG_IKPF___IBLTXT as IBLTXT",
	"Jnr_ISEG_IKPF___INVART as INVART",
	"Jnr_ISEG_IKPF___WSTI_BSTAT as WSTI_BSTAT",
	"Exp_ZTB___MANDT as MANDT2",
	"Exp_ZTB___SITE as SITE",
	"Exp_ZTB___COUNT_QTY as COUNT_QTY",
	"Exp_ZTB___ZTB_POST_DATE as ZTB_POST_DATE",
	"Exp_ZTB___ARTICLE as ARTICLE1") \
	.withColumn('ARTICLE', lit(None))
	

# COMMAND ----------

# Processing node Exp_Fields, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 81

# for each involved DataFrame, append the dataframe name to each column
Jnr_ZTB_temp = Jnr_ZTB.toDF(*["Jnr_ZTB___" + col for col in Jnr_ZTB.columns])

Exp_Fields = Jnr_ZTB_temp.selectExpr(
	"Jnr_ZTB___POSTING_DT as POSTING_DT",
	"Jnr_ZTB___DOC_NBR as DOC_NBR",
	"Jnr_ZTB___SITE_NBR as SITE_NBR",
  "cast(LTRIM('0', Jnr_ZTB___SKU_NBR) as int) as SKU_NBR",
	"Jnr_ZTB___MANDT as CLIENT",
	"Jnr_ZTB___FISCAL_YR as FISCAL_YR",
	"Jnr_ZTB___BOOK_QTY as BOOK_QTY",
	"Jnr_ZTB___PHYSICAL_QTY as PHYSICAL_QTY",
	"Jnr_ZTB___DIFF_AMT as DIFF_AMT",
	"Jnr_ZTB___VGART as TRANS_TYPE",
	"Jnr_ZTB___LGORT1 as STORAGE_LOC",
	"Jnr_ZTB___SOBKZ1 as SPECIAL_STOCK_IND",
	"Jnr_ZTB___BLDAT as DOC_DT",
	"Jnr_ZTB___GIDAT as PLANNED_DT",
	"Jnr_ZTB___ZLDAT1 as LAST_COUNT_DT",
	"Jnr_ZTB___MONAT as FISCAL_PERIOD",
	"Jnr_ZTB___USNAM1 as CHANGED_BY",
	"Jnr_ZTB___SPERR as POSTING_BLOCK",
	"Jnr_ZTB___ZSTAT as COUNT_STATUS",
	"Jnr_ZTB___DSTAT as ADJ_POSTING_STATUS",
	"Jnr_ZTB___XBLNI1 as PHYS_INV_REF_NBR",
	"Jnr_ZTB___LSTAT as DELETE_FLAG_STATUS",
	"Jnr_ZTB___XBUFI as BOOK_INV_FREEZE",
	"Jnr_ZTB___KEORD as GROUP_CRIT_TYPE",
	"Jnr_ZTB___ORDNG as GROUP_CRIT_PI",
	"Jnr_ZTB___INVNU as PHYS_INV_TYPE_DESC",
	"Jnr_ZTB___IBLTXT as PHYS_INV_DOC_DESC",
	"Jnr_ZTB___WSTI_BSTAT as INV_BALANCE_STATUS",
	"Jnr_ZTB___ZEILI as LN_NBR",
	"Jnr_ZTB___CHARG as BATCH_NBR",
	"Jnr_ZTB___BSTAR as PHYS_INV_STOCK_TYPE_ID",
	"Jnr_ZTB___KDAUF as SALES_ORDER_NBR",
	"Jnr_ZTB___KDPOS as ITEM_NBR",
	"Jnr_ZTB___KDEIN as DELIVERY_SCHED",
	"Jnr_ZTB___LIFNR as VENDOR_NBR",
	"Jnr_ZTB___KUNNR as ACCT_CUST_NBR",
	"Jnr_ZTB___PLPLA as DISTRIBUTION_DIFF",
	"Jnr_ZTB___AEDAT as CHANGED_DT",
	"Jnr_ZTB___USNAZ as COUNTED_BY",
	"Jnr_ZTB___USNAD as ADJ_POSTING_BY",
	"Jnr_ZTB___XZAEL as ITEM_COUNTED_FLAG",
	"Jnr_ZTB___XDIFF as DIFFERENCE_POSTED",
	"Jnr_ZTB___XNZAE as ITEM_RECOUNTED_FLAG",
	"Jnr_ZTB___XLOEK as ITEM_DELETED_FLAG",
	"Jnr_ZTB___XAMEI as ALT_UOM_FLAG",
	"Jnr_ZTB___XNULL as ZERO_COUNT_FLAG",
	"Jnr_ZTB___MEINS as BASE_UOM",
	"Jnr_ZTB___ERFMG as QTY_UOM",
	"Jnr_ZTB___ERFME as PHYS_INV_UOM",
	"Jnr_ZTB___MBLNR as NBR_ARTICLE_DOC",
	"Jnr_ZTB___MJAHR as ARTICLE_DOC_YR",
	"Jnr_ZTB___ZEILE as ITEM_ARTICLE_DOC",
	"Jnr_ZTB___NBLNR as NBR_RECOUNT_DOC",
	"Jnr_ZTB___WAERS as CURRENCY_CD",
	"Jnr_ZTB___ABCIN as PHYS_INV_CYCLE_COUNT_IND",
	"Jnr_ZTB___PS_PSP_PNR as WBS_ELEMENT",
	"Jnr_ZTB___VKWRT as SALES_PRICE_TAX_INC",
	"Jnr_ZTB___EXVKW as EXT_SALES_VALUE_LOCAL",
	"Jnr_ZTB___BUCHW as BOOK_VALUE_SP",
	"Jnr_ZTB___KWART as INV_FLAG",
	"Jnr_ZTB___VKWRA as SALES_PRICE_TAX_EXC",
	"Jnr_ZTB___VKMZL as SALES_INV_DIFF_VAT",
	"Jnr_ZTB___VKNZL as SALES_INV_DIFF_NOVAT",
	"Jnr_ZTB___WRTZL as PHYS_INV_COUNT_VALUE",
	"Jnr_ZTB___WRTBM as BOOK_QTY_VALUE",
	"Jnr_ZTB___DIWZL as INV_DIFF",
	"Jnr_ZTB___ATTYP as ARTCILE_CATEGORY",
	"Jnr_ZTB___GRUND as INV_DIFF_REASON",
	"Jnr_ZTB___SAMAT as XSITE_CONF_ARTICLE",
	"Jnr_ZTB___XDISPATCH as PHYS_INV_DIST_DIFF",
	"Jnr_ZTB___WSTI_COUNTDATE as DATE_COUNT_OT",
	"Jnr_ZTB___WSTI_COUNTTIME as TIME_COUNT_OT",
	"Jnr_ZTB___WSTI_FREEZEDATE as FREEZE_DT_INV_BALANCE",
	"Jnr_ZTB___WSTI_FREEZETIME as FREEZE_TIME_INV_BALANCE",
	"Jnr_ZTB___WSTI_POSM as BOOK_QTY_CHANGED",
	"Jnr_ZTB___WSTI_POSW as RETAIL_VALUE",
	"Jnr_ZTB___WSTI_XCALC as BOOK_INV",
	"Jnr_ZTB___WSTI_ENTERDATE as PHYS_INV_ENTRY_DATE",
	"Jnr_ZTB___WSTI_ENTERTIME as PHYS_INV_ENTRY_TIME",
	"Jnr_ZTB___COUNT_QTY as COUNT_QTY",
  "CURRENT_TIMESTAMP as LOAD_DT",
	"IF(Jnr_ZTB___COUNT_QTY IS NULL, 0, Jnr_ZTB___COUNT_QTY) as OVERSTOCK_QTY"
)

# COMMAND ----------

# Processing node Shortcut_to_INV_PHYSICAL_PRE, type TARGET 
# COLUMN COUNT: 81


Shortcut_to_INV_PHYSICAL_PRE = Exp_Fields.selectExpr(
	"CAST(POSTING_DT AS DATE) as POSTING_DT",
	"CAST(DOC_NBR AS STRING) as DOC_NBR",
	"CAST(SITE_NBR AS STRING) as SITE_NBR",
	"CAST(SKU_NBR AS INT) as SKU_NBR",
	"CAST(CLIENT AS STRING) as CLIENT",
	"CAST(FISCAL_YR AS INT) as FISCAL_YR",
	"CAST(BOOK_QTY AS DECIMAL(13,3)) as BOOK_QTY",
	"CAST(PHYSICAL_QTY AS DECIMAL(13,3)) as PHYSICAL_QTY",
	"CAST(OVERSTOCK_QTY AS DECIMAL(13,3)) as OVERSTOCK_QTY",
	"CAST(DIFF_AMT AS DECIMAL(13,2)) as DIFF_AMT",
	"CAST(TRANS_TYPE AS STRING) as TRANS_TYPE",
	"CAST(STORAGE_LOC AS STRING) as STORAGE_LOC",
	"CAST(SPECIAL_STOCK_IND AS STRING) as SPECIAL_STOCK_IND",
	"CAST(DOC_DT AS DATE) as DOC_DT",
	"CAST(PLANNED_DT AS DATE) as PLANNED_DT",
	"CAST(LAST_COUNT_DT AS DATE) as LAST_COUNT_DT",
	"CAST(FISCAL_PERIOD AS INT) as FISCAL_PERIOD",
	"CAST(CHANGED_BY AS STRING) as CHANGED_BY",
	"CAST(POSTING_BLOCK AS STRING) as POSTING_BLOCK",
	"CAST(COUNT_STATUS AS STRING) as COUNT_STATUS",
	"CAST(ADJ_POSTING_STATUS AS STRING) as ADJ_POSTING_STATUS",
	"CAST(PHYS_INV_REF_NBR AS STRING) as PHYS_INV_REF_NBR",
	"CAST(DELETE_FLAG_STATUS AS STRING) as DELETE_FLAG_STATUS",
	"CAST(BOOK_INV_FREEZE AS STRING) as BOOK_INV_FREEZE",
	"CAST(GROUP_CRIT_TYPE AS STRING) as GROUP_CRIT_TYPE",
	"CAST(GROUP_CRIT_PI AS STRING) as GROUP_CRIT_PI",
	"CAST(PHYS_INV_TYPE_DESC AS STRING) as PHYS_INV_TYPE_DESC",
	"CAST(PHYS_INV_DOC_DESC AS STRING) as PHYS_INV_DOC_DESC",
	"CAST(INV_BALANCE_STATUS AS STRING) as INV_BALANCE_STATUS",
	"CAST(LN_NBR AS INT) as LN_NBR",
	"CAST(BATCH_NBR AS STRING) as BATCH_NBR",
	"CAST(PHYS_INV_STOCK_TYPE_ID AS STRING) as PHYS_INV_STOCK_TYPE_ID",
	"CAST(SALES_ORDER_NBR AS STRING) as SALES_ORDER_NBR",
	"CAST(ITEM_NBR AS INT) as ITEM_NBR",
	"CAST(DELIVERY_SCHED AS INT) as DELIVERY_SCHED",
	"CAST(VENDOR_NBR AS STRING) as VENDOR_NBR",
	"CAST(ACCT_CUST_NBR AS STRING) as ACCT_CUST_NBR",
	"CAST(DISTRIBUTION_DIFF AS STRING) as DISTRIBUTION_DIFF",
	"CAST(CHANGED_DT AS DATE) as CHANGED_DT",
	"CAST(COUNTED_BY AS STRING) as COUNTED_BY",
	"CAST(ADJ_POSTING_BY AS STRING) as ADJ_POSTING_BY",
	"CAST(ITEM_COUNTED_FLAG AS STRING) as ITEM_COUNTED_FLAG",
	"CAST(DIFFERENCE_POSTED AS STRING) as DIFFERENCE_POSTED",
	"CAST(ITEM_RECOUNTED_FLAG AS STRING) as ITEM_RECOUNTED_FLAG",
	"CAST(ITEM_DELETED_FLAG AS STRING) as ITEM_DELETED_FLAG",
	"CAST(ALT_UOM_FLAG AS STRING) as ALT_UOM_FLAG",
	"CAST(ZERO_COUNT_FLAG AS STRING) as ZERO_COUNT_FLAG",
	"CAST(BASE_UOM AS STRING) as BASE_UOM",
	"CAST(QTY_UOM AS DECIMAL(13,3)) as QTY_UOM",
	"CAST(PHYS_INV_UOM AS STRING) as PHYS_INV_UOM",
	"CAST(NBR_ARTICLE_DOC AS STRING) as NBR_ARTICLE_DOC",
	"CAST(ARTICLE_DOC_YR AS INT) as ARTICLE_DOC_YR",
	"CAST(ITEM_ARTICLE_DOC AS INT) as ITEM_ARTICLE_DOC",
	"CAST(NBR_RECOUNT_DOC AS STRING) as NBR_RECOUNT_DOC",
	"CAST(CURRENCY_CD AS STRING) as CURRENCY_CD",
	"CAST(PHYS_INV_CYCLE_COUNT_IND AS STRING) as PHYS_INV_CYCLE_COUNT_IND",
	"CAST(WBS_ELEMENT AS INT) as WBS_ELEMENT",
	"CAST(SALES_PRICE_TAX_INC AS DECIMAL(13,2)) as SALES_PRICE_TAX_INC",
	"CAST(EXT_SALES_VALUE_LOCAL AS DECIMAL(13,2)) as EXT_SALES_VALUE_LOCAL",
	"CAST(BOOK_VALUE_SP AS DECIMAL(13,2)) as BOOK_VALUE_SP",
	"CAST(INV_FLAG AS STRING) as INV_FLAG",
	"CAST(SALES_PRICE_TAX_EXC AS DECIMAL(13,2)) as SALES_PRICE_TAX_EXC",
	"CAST(SALES_INV_DIFF_VAT AS DECIMAL(13,2)) as SALES_INV_DIFF_VAT",
	"CAST(SALES_INV_DIFF_NOVAT AS DECIMAL(13,2)) as SALES_INV_DIFF_NOVAT",
	"CAST(PHYS_INV_COUNT_VALUE AS DECIMAL(13,2)) as PHYS_INV_COUNT_VALUE",
	"CAST(BOOK_QTY_VALUE AS DECIMAL(13,2)) as BOOK_QTY_VALUE",
	"CAST(INV_DIFF AS DECIMAL(13,2)) as INV_DIFF",
	"CAST(ARTCILE_CATEGORY AS STRING) as ARTCILE_CATEGORY",
	"CAST(INV_DIFF_REASON AS SMALLINT) as INV_DIFF_REASON",
	"CAST(XSITE_CONF_ARTICLE AS STRING) as XSITE_CONF_ARTICLE",
	"CAST(PHYS_INV_DIST_DIFF AS STRING) as PHYS_INV_DIST_DIFF",
	"CAST(DATE_COUNT_OT AS DATE) as DATE_COUNT_OT",
	"CAST(TIME_COUNT_OT AS DATE) as TIME_COUNT_OT",
	"CAST(FREEZE_DT_INV_BALANCE AS DATE) as FREEZE_DT_INV_BALANCE",
	"CAST(FREEZE_TIME_INV_BALANCE AS DATE) as FREEZE_TIME_INV_BALANCE",
	"CAST(BOOK_QTY_CHANGED AS DECIMAL(13,3)) as BOOK_QTY_CHANGED",
	"CAST(RETAIL_VALUE AS DECIMAL(13,2)) as RETAIL_VALUE",
	"CAST(BOOK_INV AS STRING) as BOOK_INV",
	"CAST(PHYS_INV_ENTRY_DATE AS DATE) as PHYS_INV_ENTRY_DATE",
	"CAST(PHYS_INV_ENTRY_TIME AS DATE) as PHYS_INV_ENTRY_TIME",
	"CAST(LOAD_DT AS DATE) as LOAD_DT"
)

Shortcut_to_INV_PHYSICAL_PRE.write.mode("overwrite").saveAsTable(f'{raw}.INV_PHYSICAL_PRE')
