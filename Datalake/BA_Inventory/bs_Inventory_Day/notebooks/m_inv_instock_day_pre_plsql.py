# Databricks notebook source
#Code converted on 2023-09-15 14:55:20
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

_sql = f"""INSERT OVERWRITE {raw}.INV_INSTOCK_PRE
SELECT current_date - INTERVAL 1 DAY AS DAY_DT
	,POGLST.PRODUCT_ID
	,POGLST.LOCATION_ID
	,0 AS AVG_QTY
	,MIN(OUT_OF_STOCK_IND) AS OUT_OF_STOCK_IND
	,MAX(POG_LISTED_FLAG) AS POG_LISTED_IND
	,MAX(SAP_LISTED_IND) AS SAP_LISTED_IND
	,MAX(INLINE_IND) AS INLINE_IND
	,MAX(PLANNER_IND) AS PLANNER_IND
	,MAX(SUBS_IND) AS SUBS_IND
	,MAX(NVL(SKU_CAPACITY_QTY, 0)) SKU_CAPACITY_QTY
	,CURRENT_DATE AS LOAD_DT
	,MAX(NVL(SKU_FACINGS_QTY, 0)) SKU_FACINGS_QTY
FROM (
	SELECT NVL(SKUSUB.PRODUCT_ID, POGLST.PRODUCT_ID) AS PRODUCT_ID
		,POGLST.LOCATION_ID AS LOCATION_ID
		,0 AS AVG_QTY
		,NVL2(INVDAY.PRODUCT_ID, 0, 1) AS OUT_OF_STOCK_IND
		,POGLST.POG_LISTED_FLAG
		,CASE 
			WHEN SKUSUB.PRODUCT_ID IS NULL
				THEN POGLST.SAP_LISTED_FLAG
			WHEN SKUSUB.PRODUCT_ID = SKUSUB.JOIN_PRODUCT_ID
				THEN POGLST.SAP_LISTED_FLAG
			ELSE 0
			END AS SAP_LISTED_IND
		,CASE 
			WHEN SKUSUB.PAR_FLAG IS NULL
				THEN POGLST.INLINE_FLAG
			WHEN SKUSUB.PRODUCT_ID = SKUSUB.JOIN_PRODUCT_ID
				THEN POGLST.INLINE_FLAG
			ELSE 0
			END AS INLINE_IND
		,CASE 
			WHEN SKUSUB.PAR_FLAG IS NULL
				THEN POGLST.PLANNER_FLAG
			WHEN SKUSUB.PRODUCT_ID = SKUSUB.JOIN_PRODUCT_ID
				THEN POGLST.PLANNER_FLAG
			ELSE 0
			END AS PLANNER_IND
		,NVL(SKUSUB.SUBS_FLAG, 0) AS SUBS_IND
	FROM (
		SELECT POG.PRODUCT_ID
			,POG.LOCATION_ID
			,NVL(LIST.CURR_SAP_LISTED_FLAG, 0) AS SAP_LISTED_FLAG
			,1 AS POG_LISTED_FLAG
			,POG.INLINE_FLAG
			,POG.PLANNER_FLAG
		FROM (
			SELECT P_I.PRODUCT_ID
				,--POG
				P_I.LOCATION_ID
				,CASE 
					WHEN MIN(P_I.POG_TYPE_CD) = 'F'
						THEN 1
					WHEN MIN(P_I.POG_TYPE_CD) = 'I'
						AND MAX(P_I.POG_D7_CD) = 'I'
						THEN 1
					ELSE 0
					END AS INLINE_FLAG
				,CASE 
					WHEN MAX(P_I.POG_TYPE_CD) = 'P'
						THEN 1
					ELSE 0
					END AS PLANNER_FLAG
			FROM (
				SELECT SKU.PRODUCT_ID
					,-- P_I
					SITE.LOCATION_ID
					,PP.POG_TYPE_CD
					,'I' AS POG_D7_CD
				FROM {legacy}.pog_sku_store pzp  --replacement for ztb promo
					,{legacy}.planogram_pro pp
					,{legacy}.pog_sku_pro pskp
					,{legacy}.pog_store_pro pstp
					,{legacy}.sku_profile sku
					,{legacy}.site_profile site
				WHERE pzp.POG_DBKEY = pp.POG_DBKEY
					AND current_date - INTERVAL 1 DAY BETWEEN PZP.LISTING_START_DT
						AND PP.EFF_END_DT
					AND pp.POG_TYPE_CD = 'P'  --added to exclude inline
					AND pzp.DELETE_FLAG = 0
					AND pzp.product_id = sku.product_id
					AND pzp.location_id = site.location_id
					AND pstp.pog_id = pp.pog_id
                    AND pstp.location_id = site.location_id
                    AND pskp.pog_id = pp.pog_id
                    AND pskp.product_id = sku.product_id
                    AND PP.pog_status <> 'D'  --added to include active pogs
                    AND pskp.pog_status <> 'D'
                    AND pstp.pog_status <> 'D'

				
				UNION ALL
				
				SELECT tmp.PRODUCT_ID
					,tmp.LOCATION_ID
					,CASE 
						WHEN tmp.POG_TYPE_CD <> 'I'
							THEN tmp.POG_TYPE_CD
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag = 'X'
							THEN 'J'
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag IS NULL
							THEN 'J'
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag <> 'X'
							AND d.negate_flag IS NOT NULL
							AND d.listing_eff_dt + INTERVAL 28 DAY <= current_date - INTERVAL 1 DAY
							AND p_status = 14
							THEN tmp.POG_TYPE_CD
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag <> 'X'
							AND d.negate_flag IS NOT NULL
							AND d.listing_eff_dt + INTERVAL 28 DAY <= current_date - INTERVAL 1 DAY
							AND p_status = 30
							THEN 'F'
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag <> 'X'
							AND d.negate_flag IS NOT NULL
							AND d.listing_eff_dt + INTERVAL 28 DAY > current_date - INTERVAL 1 DAY
							THEN 'J'
						END AS POG_TYPE_CD
					,CASE 
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag IS NOT NULL
							AND d.negate_flag <> 'X'
							AND d.listing_eff_dt + INTERVAL 28 DAY > current_date - INTERVAL 1 DAY
							AND p_status = 30
							THEN 'J'
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag = 'X'
							THEN 'J'
						ELSE 'I'
						END AS POG_D7_CD
				FROM (
					SELECT PSK.PRODUCT_ID
						,PST.LOCATION_ID
						,s.SKU_NBR
						,si.STORE_NBR
						,pp.POG_TYPE_CD
						,CASE 
							WHEN UPPER(SUBSTR(pp.pog_nbr, 1, 2)) IN (
									SELECT UPPER(status_id)
									FROM {legacy}.pog_status
									)
								THEN 30
							ELSE 14
							END AS p_status
					FROM {legacy}.pog_sku_pro PSK
						,{legacy}.pog_store_pro PST
						,{legacy}.planogram_pro PP
						,{legacy}.sku_profile s
						,{legacy}.site_profile si
					WHERE PSK.POG_ID = PP.POG_ID
						AND PSK.POG_ID = PST.POG_ID
						AND PP.POG_ID = PST.POG_ID
						AND (
							PP.POG_TYPE_CD = 'I'
							AND PSK.POG_STATUS <> 'D'
							AND PST.POG_STATUS <> 'D'
							AND PP.POG_STATUS <> 'D'
							)
						AND s.PRODUCT_ID = PSK.PRODUCT_ID
						AND si.location_id = PST.LOCATION_ID
					) tmp
				LEFT OUTER JOIN {legacy}.listing_day d ON tmp.sku_nbr = d.SKU_NBR
					AND tmp.store_nbr = d.STORE_NBR
				) P_I
			GROUP BY P_I.PRODUCT_ID
				,P_I.LOCATION_ID
			) POG
		LEFT OUTER JOIN {legacy}.sku_site_profile LIST ON POG.PRODUCT_ID = LIST.PRODUCT_ID
			AND POG.LOCATION_ID = LIST.LOCATION_ID
		
		UNION ALL
		
		SELECT LIST.PRODUCT_ID
			,LIST.LOCATION_ID
			,LIST.CURR_SAP_LISTED_FLAG AS SAP_LISTED_FLAG
			,0 AS POG_LISTED_FLAG
			,0 AS INLINE_FLAG
			,0 AS PLANNER_FLAG
		FROM {legacy}.sku_site_profile LIST
		LEFT OUTER JOIN (
			SELECT P_I.PRODUCT_ID
				,P_I.LOCATION_ID
				,CASE 
					WHEN MIN(P_I.POG_TYPE_CD) = 'F'
						THEN 1
					WHEN MIN(P_I.POG_TYPE_CD) = 'I'
						AND MAX(P_I.POG_D7_CD) = 'I'
						THEN 1
					ELSE 0
					END AS INLINE_FLAG
				,CASE 
					WHEN MAX(P_I.POG_TYPE_CD) = 'P'
						THEN 1
					ELSE 0
					END AS PLANNER_FLAG
			FROM (
				SELECT /*+ ORDERED USE_HASH (PZP, PP, SKU, SITE) */
					SKU.PRODUCT_ID
					,SITE.LOCATION_ID
					,PP.POG_TYPE_CD
					,'I' AS POG_D7_CD
				FROM {legacy}.pog_sku_store pzp
					,{legacy}.planogram_pro pp
					,{legacy}.pog_sku_pro pskp
					,{legacy}.pog_store_pro pstp
					,{legacy}.sku_profile sku
					,{legacy}.site_profile site
				WHERE pzp.POG_DBKEY = pp.POG_DBKEY
					AND current_date - INTERVAL 1 DAY BETWEEN PZP.LISTING_START_DT
						AND PP.EFF_END_DT
					AND pp.POG_TYPE_CD = 'P'
					AND pzp.DELETE_FLAG = 0
					AND pzp.product_id = sku.product_id
					AND pzp.location_id = site.location_id
					AND pstp.pog_id = pp.pog_id
                    AND pstp.location_id = site.location_id
                    AND pskp.pog_id = pp.pog_id
                    AND pskp.product_id = sku.product_id
                    AND PP.pog_status <> 'D'
                    AND pskp.pog_status <> 'D'
                    AND pstp.pog_status <> 'D'					
				
				UNION ALL
				
				SELECT tmp.PRODUCT_ID
					,tmp.LOCATION_ID
					,CASE 
						WHEN tmp.POG_TYPE_CD <> 'I'
							THEN tmp.POG_TYPE_CD
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag = 'X'
							THEN 'J'
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag IS NULL
							THEN 'J'
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag <> 'X'
							AND d.negate_flag IS NOT NULL
							AND d.listing_eff_dt + INTERVAL 28 DAY <= current_date - INTERVAL 1 DAY
							AND p_status = 14
							THEN tmp.POG_TYPE_CD
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag <> 'X'
							AND d.negate_flag IS NOT NULL
							AND d.listing_eff_dt + INTERVAL 28 DAY <= current_date - INTERVAL 1 DAY
							AND p_status = 30
							THEN 'F'
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag <> 'X'
							AND d.negate_flag IS NOT NULL
							AND d.listing_eff_dt + INTERVAL 28 DAY > current_date - INTERVAL 1 DAY
							THEN 'J'
						END AS POG_TYPE_CD
					,CASE 
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag IS NOT NULL
							AND d.negate_flag <> 'X'
							AND d.listing_eff_dt + INTERVAL 28 DAY > current_date - INTERVAL 1 DAY
							AND p_status = 30
							THEN 'J'
						WHEN tmp.POG_TYPE_CD = 'I'
							AND d.negate_flag = 'X'
							THEN 'J'
						ELSE 'I'
						END AS POG_D7_CD
				FROM (
					SELECT PSK.PRODUCT_ID
						,PST.LOCATION_ID
						,s.SKU_NBR
						,si.STORE_NBR
						,pp.POG_TYPE_CD
						,CASE 
							WHEN UPPER(SUBSTR(pp.pog_nbr, 1, 2)) IN (
									SELECT UPPER(status_id)
									FROM {legacy}.pog_status
									)
								THEN 30
							ELSE 14
							END AS p_status
					FROM {legacy}.pog_sku_pro PSK
						,{legacy}.pog_store_pro PST
						,{legacy}.planogram_pro PP
						,{legacy}.sku_profile s
						,{legacy}.site_profile si
					WHERE PSK.POG_ID = PP.POG_ID
						AND PSK.POG_ID = PST.POG_ID
						AND PP.POG_ID = PST.POG_ID
						AND (
							PP.POG_TYPE_CD = 'I'
							AND PSK.POG_STATUS <> 'D'
							AND PST.POG_STATUS <> 'D'
							AND PP.POG_STATUS <> 'D'
							)
						AND s.PRODUCT_ID = PSK.PRODUCT_ID
						AND si.location_id = PST.LOCATION_ID
					) tmp
				LEFT OUTER JOIN {legacy}.listing_day d ON tmp.sku_nbr = d.SKU_NBR
					AND tmp.store_nbr = d.STORE_NBR
				) P_I
			GROUP BY P_I.PRODUCT_ID
				,P_I.LOCATION_ID
			) POG ON LIST.PRODUCT_ID = POG.PRODUCT_ID
			AND LIST.LOCATION_ID = POG.LOCATION_ID
		WHERE (LIST.HIST_INV_FLAG + LIST.CURR_SAP_LISTED_FLAG + LIST.CURR_POG_LISTED_FLAG > 0)
			AND POG.PRODUCT_ID IS NULL
		) POGLST
	LEFT OUTER JOIN (
		SELECT PRODUCT_ID
			,PRODUCT_ID AS JOIN_PRODUCT_ID
			,CASE 
				WHEN SUBS_IND = 0
					THEN 1
				ELSE 0
				END AS PAR_FLAG
			,1 AS SUBS_FLAG
		FROM {legacy}.sku_subs_view
		
		UNION
		
		SELECT PRODUCT_ID
			,SUBS_PRODUCT_ID AS JOIN_PRODUCT_ID
			,CASE 
				WHEN SUBS_IND = 0
					THEN 1
				ELSE 0
				END AS PAR_FLAG
			,1 AS SUBS_FLAG
		FROM {legacy}.sku_subs_view
		) SKUSUB ON POGLST.PRODUCT_ID = SKUSUB.JOIN_PRODUCT_ID
	LEFT OUTER JOIN (
		SELECT INV.DAY_DT
			,INV.PRODUCT_ID
			,INV.SKU_NBR
			,INV.LOCATION_ID
			,INV.ON_HAND_QTY
			,ISS.QUANTITY
		FROM (
			SELECT DAY_DT
				,SKU.PRODUCT_ID
				,IP.SKU_NBR
				,SP.LOCATION_ID
				,IP.ON_HAND_QTY
			FROM {raw}.inventory_pre IP
				,{legacy}.sku_profile SKU
				,{legacy}.site_profile SP
			WHERE IP.SKU_NBR = SKU.SKU_NBR
				AND IP.STORE_NBR = SP.STORE_NBR
			) INV
		LEFT OUTER JOIN {legacy}.inventory_event_str_sku ISS ON INV.LOCATION_ID = ISS.LOCATION_ID
			AND INV.PRODUCT_ID = ISS.PRODUCT_ID
		) INVDAY ON POGLST.PRODUCT_ID = INVDAY.PRODUCT_ID
		AND POGLST.LOCATION_ID = INVDAY.LOCATION_ID
		AND DATE_TRUNC('DAY', current_date - INTERVAL 1 DAY) = INVDAY.DAY_DT
		AND INVDAY.ON_HAND_QTY - NVL(INVDAY.QUANTITY, 0) > 0
	) POGLST
LEFT OUTER JOIN (
	SELECT PRODUCT_ID
		,LOCATION_ID
		,SUM(SKU_CAPACITY_QTY) SKU_CAPACITY_QTY
		,SUM(SKU_FACINGS_QTY) SKU_FACINGS_QTY
	FROM {legacy}.pog_sku_store_pro
	GROUP BY PRODUCT_ID
		,LOCATION_ID
	) sku_capacity ON POGLST.PRODUCT_ID = SKU_CAPACITY.PRODUCT_ID
	AND POGLST.LOCATION_ID = sku_capacity.LOCATION_ID
GROUP BY POGLST.PRODUCT_ID
	,POGLST.LOCATION_ID
HAVING MIN(OUT_OF_STOCK_IND) = 0
	OR (MAX(POG_LISTED_FLAG) + MAX(SAP_LISTED_IND) + MAX(INLINE_IND) + MAX(PLANNER_IND) + MAX(SUBS_IND)) > 0"""

spark.sql(_sql)

# COMMAND ----------


