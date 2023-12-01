# Databricks notebook source
#Code converted on 2023-10-11 11:42:48
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

# Processing node ASQ_Shortcut_To_SALES_WEEK_SKU_STORE, type SOURCE 
# COLUMN COUNT: 25

_sql = f"""
SELECT
    DATE_ADD(CURRENT_DATE, -(date_part('dow', CURRENT_DATE) - 1)) AS week_dt,
    pdp.product_id,
    pdp.location_id,
    ifnull(sdss.sales_amt,0) as sales_amt,
    ifnull(sdss.sales_cost,0) as sales_cost,
    ifnull(sdss.sales_qty,0) as sales_qty,
    ifnull(sdss.return_amt,0) as return_amt,
    ifnull(sdss.return_cost,0) as return_cost,
    ifnull(sdss.return_qty,0) as return_qty,
    ifnull(sdss.discount_amt,0) as discount_amt,
    ifnull(sdss.discount_return_amt,0) as discount_return_amt,
    ifnull(sdss.pos_coupon_amt,0) as pos_coupon_amt,
    ifnull(sdss.special_sales_amt,0) as special_sales_amt,
    ifnull(sdss.special_sales_qty,0) as special_sales_qty,
    ifnull(sdss.special_return_amt,0) as special_return_amt,
    ifnull(sdss.special_return_qty,0) as special_return_qty,
    ifnull(pawsp.avg_wk_sales_qty,0) as avg_wk_sales_qty,
    pdp.pog_id,
    pdp.pog_type_cd,
    ifnull(pdp.inline_cnt,0) as inline_cnt,
    ifnull(pdp.planner_cnt,0) as planner_cnt,
    ifnull(pdp.prodloc_cnt,0) as prodloc_cnt,
    ifnull(pdp.prodloc_promo_qty,0) as prodloc_promo_qty,
    ifnull(pmdwp.merch_discount_amt,0) as merch_discount_amt,
    ifnull(pmdwp.merch_discount_return_amt,0) as merch_discount_return_amt
FROM
    {raw}.pog_distrib_pre pdp
LEFT OUTER JOIN
    {raw}.pog_avg_week_sales_pre pawsp
ON
    pdp.product_id = pawsp.product_id
    AND pdp.location_id = pawsp.location_id
LEFT OUTER JOIN (
    SELECT
        product_id,
        location_id,
        SUM(sales_amt) AS sales_amt,
        SUM(sales_cost) AS sales_cost,
        SUM(sales_qty) AS sales_qty,
        SUM(return_amt) AS return_amt,
        SUM(return_cost) AS return_cost,
        SUM(return_qty) AS return_qty,
        SUM(discount_amt) AS discount_amt,
        SUM(discount_return_amt) AS discount_return_amt,
        SUM(pos_coupon_amt) AS pos_coupon_amt,
        SUM(special_sales_amt) AS special_sales_amt,
        SUM(special_sales_qty) AS special_sales_qty,
        SUM(special_return_amt) AS special_return_amt,
        SUM(special_return_qty) AS special_return_qty
    FROM
        {legacy}.sales_day_sku_store
    WHERE
        week_dt = DATE_ADD(CURRENT_DATE, -(date_part('dow', CURRENT_DATE) - 1))
    GROUP BY
        product_id,
        location_id
) sdss
ON
    pdp.product_id = sdss.product_id
    AND pdp.location_id = sdss.location_id
LEFT OUTER JOIN
    {raw}.pog_merch_discount_week_pre pmdwp
ON
    pdp.product_id = pmdwp.product_id
    AND pdp.location_id = pmdwp.location_id
"""


ASQ_Shortcut_To_SALES_WEEK_SKU_STORE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_SALES_WEEK_SKU_STORE = ASQ_Shortcut_To_SALES_WEEK_SKU_STORE \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[0],'WEEK_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[2],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[3],'SALES_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[4],'SALES_COST') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[5],'SALES_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[6],'RETURN_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[7],'RETURN_COST') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[8],'RETURN_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[9],'DISCOUNT_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[10],'DISCOUNT_RETURN_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[11],'POS_COUPON_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[12],'SPECIAL_SALES_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[13],'SPECIAL_SALES_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[14],'SPECIAL_RETURN_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[15],'SPECIAL_RETURN_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[16],'AVG_WK_SALES_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[17],'POG_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[18],'POG_TYPE_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[19],'INLINE_CNT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[20],'PLANNER_CNT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[21],'PRODLOC_CNT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[22],'PRODLOC_PROMO_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[23],'MERCH_DISCOUNT_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[24],'MERCH_DISCOUNT_RETURN_AMT')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 14

EXPTRANS = ASQ_Shortcut_To_SALES_WEEK_SKU_STORE \
    .withColumn("POG_MULTI_INLINE_FLAG", expr("IF (INLINE_CNT > 1, 'Y', 'N')")) \
    .withColumn("POG_MULTI_PLANNER_FLAG", expr("IF (PLANNER_CNT > 1, 'Y', 'N')")) \
    .withColumn("NET_SALES_AMT_VAR", expr("SALES_AMT - RETURN_AMT - ( DISCOUNT_AMT - DISCOUNT_RETURN_AMT ) - ( SPECIAL_SALES_AMT - SPECIAL_RETURN_AMT ) - POS_COUPON_AMT")) \
	.withColumn("NET_SALES_COST_VAR", expr("SALES_COST - RETURN_COST")) \
	.withColumn("SALES_ABOVE_AVG_QTY", expr("IF (SALES_QTY <= AVG_WK_SALES_QTY, 0, cast(SALES_QTY - AVG_WK_SALES_QTY as int))")) \
	.withColumn("MAX_WK_SALES_QTY", expr("IF (SALES_QTY <= AVG_WK_SALES_QTY, SALES_QTY, AVG_WK_SALES_QTY)")) \
	.withColumn("NET_SALES_QTY_VAR", expr("SALES_QTY - RETURN_QTY - ( SPECIAL_SALES_QTY - SPECIAL_RETURN_QTY )")) \
     .withColumn("cond1",expr("case when POG_TYPE_CD = 'I' and INLINE_CNT>=1 THEN 1/INLINE_CNT "
                         "when POG_TYPE_CD = 'P' and INLINE_CNT=0 THEN 1/PLANNER_CNT "
                         "else null end"))\
    .withColumn("cond2",expr("case when POG_TYPE_CD = 'I'  THEN 1/INLINE_CNT*MAX_WK_SALES_QTY/SALES_QTY "
                         "when POG_TYPE_CD = 'P' THEN (1/PLANNER_CNT)*SALES_ABOVE_AVG_QTY/SALES_QTY "
                         "else null end"))\
	.withColumn("DISTRIB_PRCNTG", expr(
                                        "CASE WHEN SALES_QTY = 0 THEN cond1 "
                                        "WHEN INLINE_CNT >= 1 AND PLANNER_CNT = 0 THEN 1/ INLINE_CNT "
                                        "WHEN INLINE_CNT = 0 AND PLANNER_CNT >= 1 THEN 1/ PLANNER_CNT "
                                        "WHEN INLINE_CNT >= 1 AND PLANNER_CNT >= 1 THEN cond2 "
                                        "ELSE NULL END"
                                    )) \
	.withColumn("MERCH_SALES_AMT_VAR", expr("SALES_AMT - RETURN_AMT - ( MERCH_DISCOUNT_AMT - MERCH_DISCOUNT_RETURN_AMT ) - ( SPECIAL_SALES_AMT - SPECIAL_RETURN_AMT ) - POS_COUPON_AMT")) \
	.withColumn("MERCH_SALES_AMT", expr("MERCH_SALES_AMT_VAR * DISTRIB_PRCNTG * 1.000")) \
	.withColumn("var_NET_SALES_AMT", expr("NET_SALES_AMT_VAR * DISTRIB_PRCNTG * 1.000")) \
	.withColumn("var_NET_SALES_COST", expr("NET_SALES_COST_VAR * DISTRIB_PRCNTG * 1.000")) \
    .withColumn("NET_SALES_QTY", expr("NET_SALES_QTY_VAR * DISTRIB_PRCNTG * 1.000"))

# COMMAND ----------

EXP_ROUND = EXPTRANS \
    .withColumn("inp_MERCH_SALES_AMT", col("MERCH_SALES_AMT")) \
    .withColumn("var_MERCH_SALES_AMT", expr("CAST(inp_MERCH_SALES_AMT AS decimal(38, 4))")) \
    .withColumn("var_POINT_POS", expr("INSTR(inp_MERCH_SALES_AMT, '.')")) \
    .withColumn("var_3RD_DIGIT", expr("SUBSTRING(inp_MERCH_SALES_AMT, var_POINT_POS + 3, 1)")) \
    .withColumn("var_4TH_DIGIT", expr("SUBSTRING(inp_MERCH_SALES_AMT, var_POINT_POS + 4, 1)")) \
    .withColumn("var_MERCH_SALES_AMT_CALC",
                expr("""
                    CASE
                        WHEN var_3RD_DIGIT = '5' AND var_MERCH_SALES_AMT > 0 THEN var_MERCH_SALES_AMT + 0.005
                        WHEN var_3RD_DIGIT = '5' AND var_MERCH_SALES_AMT < 0 THEN var_MERCH_SALES_AMT - 0.005
                        WHEN var_3RD_DIGIT = '4' AND var_4TH_DIGIT >= '5' AND var_MERCH_SALES_AMT > 0 THEN var_MERCH_SALES_AMT + 0.005
                        WHEN var_3RD_DIGIT = '4' AND var_4TH_DIGIT >= '5' AND var_MERCH_SALES_AMT < 0 THEN var_MERCH_SALES_AMT - 0.005
                        ELSE var_MERCH_SALES_AMT
                    END
                """)) \
    .withColumn("MERCH_SALES_AMT", expr("ROUND(var_MERCH_SALES_AMT_CALC, 2)"))

# COMMAND ----------

EXP_ROUND1 = EXP_ROUND \
    .withColumn("inp_NET_SALES_AMT", col("var_NET_SALES_AMT")) \
    .withColumn("var_NET_SALES_AMT", expr("CAST(inp_NET_SALES_AMT AS decimal(38, 4))")) \
    .withColumn("var_POINT_POS", expr("INSTR(inp_NET_SALES_AMT, '.')")) \
    .withColumn("var_3RD_DIGIT", expr("SUBSTRING(inp_NET_SALES_AMT, var_POINT_POS + 3, 1)")) \
    .withColumn("var_4TH_DIGIT", expr("SUBSTRING(inp_NET_SALES_AMT, var_POINT_POS + 4, 1)")) \
    .withColumn("var_NET_SALES_AMT_CALC",
                expr("""
                    CASE
                        WHEN var_3RD_DIGIT = '5' AND var_NET_SALES_AMT > 0 THEN var_NET_SALES_AMT + 0.005
                        WHEN var_3RD_DIGIT = '5' AND var_NET_SALES_AMT < 0 THEN var_NET_SALES_AMT - 0.005
                        WHEN var_3RD_DIGIT = '4' AND var_4TH_DIGIT >= '5' AND var_NET_SALES_AMT > 0 THEN var_NET_SALES_AMT + 0.005
                        WHEN var_3RD_DIGIT = '4' AND var_4TH_DIGIT >= '5' AND var_NET_SALES_AMT < 0 THEN var_NET_SALES_AMT - 0.005
                        ELSE var_NET_SALES_AMT
                    END
                """)) \
    .withColumn("NET_SALES_AMT", expr("ROUND(var_NET_SALES_AMT_CALC, 2)"))
    

# COMMAND ----------

# Processing node EXP_ROUN_NET_SALES_COST, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 1

EXP_ROUN_NET_SALES_COST = EXP_ROUND1 \
    .withColumn("inp_NET_SALES_COST", col("var_NET_SALES_COST")) \
    .withColumn("var_NET_SALES_COST", expr("CAST(inp_NET_SALES_COST AS decimal(38, 4))")) \
    .withColumn("var_POINT_POS", expr("INSTR(inp_NET_SALES_COST, '.')")) \
    .withColumn("var_3RD_DIGIT", expr("SUBSTRING(inp_NET_SALES_COST, var_POINT_POS + 3, 1)")) \
    .withColumn("var_4TH_DIGIT", expr("SUBSTRING(inp_NET_SALES_COST, var_POINT_POS + 4, 1)")) \
    .withColumn("var_NET_SALES_COST_TEMP", lit(0.0050)) \
    .withColumn("var_NET_SALES_COST_CALC",
                expr("""
                    CASE
                        WHEN var_NET_SALES_COST = var_NET_SALES_COST_TEMP OR var_NET_SALES_COST = -var_NET_SALES_COST_TEMP THEN 0
                        WHEN var_3RD_DIGIT = '5' AND var_NET_SALES_COST > 0 THEN var_NET_SALES_COST + 0.005
                        WHEN var_3RD_DIGIT = '5' AND var_NET_SALES_COST < 0 THEN var_NET_SALES_COST - 0.005
                        WHEN var_3RD_DIGIT = '4' AND var_4TH_DIGIT >= '5' AND var_NET_SALES_COST > 0 THEN var_NET_SALES_COST + 0.005
                        WHEN var_3RD_DIGIT = '4' AND var_4TH_DIGIT >= '5' AND var_NET_SALES_COST < 0 THEN var_NET_SALES_COST - 0.005
                        ELSE var_NET_SALES_COST
                    END
                """)) \
    .withColumn("NET_SALES_COST", expr("ROUND(var_NET_SALES_COST_CALC, 2)"))


# COMMAND ----------

Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE = EXP_ROUN_NET_SALES_COST.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(POG_ID AS INT) as POG_ID",
	"CAST(NET_SALES_AMT AS DECIMAL(8,2)) as NET_SALES_AMT",
	"CAST(NET_SALES_COST AS DECIMAL(8,2)) as NET_SALES_COST",
	"CAST(NET_SALES_QTY AS DECIMAL(10,3)) as NET_SALES_QTY",
	"CAST(MERCH_SALES_AMT AS DECIMAL(8,2)) as MERCH_SALES_AMT",
	"CAST(POG_TYPE_CD AS STRING) as POG_TYPE_CD",
	"CAST(POG_MULTI_INLINE_FLAG AS STRING) as POG_MULTI_INLINE_FLAG",
	"CAST(POG_MULTI_PLANNER_FLAG AS STRING) as POG_MULTI_PLANNER_FLAG",
	"CAST(PRODLOC_PROMO_QTY AS INT) as PRODLOC_PROMO_QTY",
	"CAST(INLINE_CNT AS INT) as INLINE_CNT",
	"CAST(PLANNER_CNT AS INT) as PLANNER_CNT"
)

if has_duplicates(Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE, ["WEEK_DT", "PRODUCT_ID", "LOCATION_ID", "POG_ID"]):
    raise Exception("Duplicates found in the dataset")

Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.POG_SALES_WEEK_SKU_STORE_PRE')

# COMMAND ----------


