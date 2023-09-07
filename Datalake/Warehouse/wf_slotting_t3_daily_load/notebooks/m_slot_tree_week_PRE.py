# Databricks notebook source
# Code converted on 2023-08-24 09:26:45
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username,password,connection_string) = or_kro_read_edhp1(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE, type SOURCE 
# COLUMN COUNT: 51

SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE = jdbcOracleConnection(f"""select 
sp.LOCATION_ID,
st.NODE_ID,
st.SLOT_ID,
s.ht,
s.width,
s.depth,
s.wt_limit,
s.rack_type,
s.rt_name,
st.WEEK_DT,
st.DAY_OF_WK_NBR,
st.LVL,
st.TREE_ORDER,
st.TREE,
st.NODE_NAME,
st.NODE1,
st.NODE2,
st.NODE3,
st.NODE4,
st.NODE5,
st.NODE6,
sku.PRODUCT_ID,
st.sku_name,
im.SKU_DESC,
im.WH_TI,
im.WH_HI,
im.CASE_HT,
im.CASE_LEN,
im.CASE_WID,
im.CASE_WT,
im.EACH_HT,
im.EACH_LEN,
im.EACH_WID,
im.EACH_WT,
st.SLOTITEM_ID,
st.CONST_GRP_ID, 
cg.CNSTR_GRP_NAME,
st.GRP_GRP_ID, 
st.SEQ_GRP_ID,
st.SCORE, 
st.LEGAL_FIT, 
st.LEGAL_FIT_REASON,
st.COLOR_ID, 
st.COLOR_DESC,
nvl(sg.cat_code,'00') group_cd, 
sg.cat_description grp_desc,
nvl(pt.cat_code,'00') pkg_cd, 
pt.pkg_typ_desc,
nvl(sc.cat_code,'00') status_cd, 
sc.cat_description stat_desc,
trim(s.dsp_slot) dsp_slot

from oldadmin.slot_tree_detail_pre st
left join wmspadm.sl_cnstr_grp cg on st.WHSE = cg.WHSE and st.CONST_GRP_ID = cg.CNSTR_GRP_ID
left join wmspadm.sl_item_master im on st.WHSE = im.WHSE and st.SKU_ID	= im.SKU_ID
join biwpadm.site_profile sp on st.whse = sp.store_nbr
left join biwpadm.sku_profile sku on st.SKU_NAME = sku.SKU_NBR
left join (select s.WHSE, s.SLOT_ID, s.DAY_OF_WK_NBR, 
		s.ht,   s.width,	  s.depth,  s.wt_limit, s.DSP_SLOT,
		s.rack_type, t.RT_NAME 
		from wmspadm.sl_slot s,
		wmspadm.sl_rack_type t
		where s.WHSE = t.WHSE
		and s.RACK_TYPE = t.RACK_TYPE
		and s.day_of_wk_nbr = to_char(sysdate-1,'D')  and s.del_flg=0
		order by 1, 2, 3) s 
on st.whse	= s.whse and st.slot_id= s.slot_id and st.day_of_wk_nbr = s.day_of_wk_nbr
left join (select x1.whse, x1.sku_id, c1.cat_code, c1.CAT_DESCRIPTION
		from wmspadm.sl_item_cat_xref x1,
		wmspadm.sl_cat_code c1 
		where x1.WHSE	     = c1.WHSE
		and x1.CAT_CODE_ID = c1.CAT_CODE_ID
		and x1.CAT_ID		 = c1.CAT_ID
		and x1.cat_id=5 and x1.del_flg=0 and c1.DEL_FLG=0) sg 
on st.WHSE   = sg.whse and st.SKU_ID = sg.SKU_ID
left join (select x1.whse, x1.sku_id, c1.cat_code, substr(c1.CAT_DESCRIPTION,1,20) pkg_typ_desc
		from wmspadm.sl_item_cat_xref x1,
		wmspadm.sl_cat_code c1 
		where x1.WHSE	     = c1.WHSE
		and x1.CAT_CODE_ID = c1.CAT_CODE_ID
		and x1.CAT_ID		 = c1.CAT_ID
		and x1.cat_id=6 and x1.del_flg=0 and c1.DEL_FLG=0) pt 
on st.whse	= pt.whse and st.SKU_ID = pt.sku_id
left join (select x1.whse, x1.sku_id, c1.cat_code, c1.CAT_DESCRIPTION
		from wmspadm.sl_item_cat_xref x1,
		wmspadm.sl_cat_code c1 
		where x1.WHSE	     = c1.WHSE
		and x1.CAT_CODE_ID = c1.CAT_CODE_ID
		and x1.CAT_ID		 = c1.CAT_ID
		and x1.cat_id=4 and x1.del_flg=0 and c1.DEL_FLG=0) sc 
on st.whse	= sc.whse and st.SKU_ID = sc.sku_id""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE = SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[1],'NODE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[2],'SLOT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[3],'HT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[4],'WIDTH') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[5],'DEPTH') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[6],'WT_LIMIT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[7],'RACK_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[8],'RT_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[9],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[10],'DAY_OF_WK_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[11],'LVL') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[12],'TREE_ORDER') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[13],'TREE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[14],'NODE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[15],'NODE1') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[16],'NODE2') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[17],'NODE3') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[18],'NODE4') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[19],'NODE5') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[20],'NODE6') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[21],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[22],'SKU_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[23],'SKU_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[24],'WH_TI') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[25],'WH_HI') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[26],'CASE_HT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[27],'CASE_LEN') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[28],'CASE_WID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[29],'CASE_WT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[30],'EACH_HT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[31],'EACH_LEN') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[32],'EACH_WID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[33],'EACH_WT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[34],'SLOTITEM_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[35],'CONST_GRP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[36],'CNSTR_GRP_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[37],'GRP_GRP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[38],'SEQ_GRP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[39],'SCORE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[40],'LEGAL_FIT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[41],'LEGAL_FIT_REASON') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[42],'COLOR_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[43],'COLOR_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[44],'CAT_VALUE_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[45],'CAT_DESCRIPTION') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[46],'CAT_VALUE_TYPE1') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[47],'CAT_DESCRIPTION1') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[48],'CAT_VALUE_TYPE2') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[49],'CAT_DESCRIPTION2') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.columns[50],'DSP_SLOT')

# COMMAND ----------

# Processing node Shortcut_to_SLOT_TREE_WEEK_PRE, type TARGET 
# COLUMN COUNT: 51


Shortcut_to_SLOT_TREE_WEEK_PRE = SQ_Shortcut_to_SLOT_TREE_DETAIL_PRE.selectExpr(
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(NODE_ID AS INT) as NODE_ID",
	"CAST(SLOT_ID AS INT) as SLOT_ID",
	"CAST(HT AS DECIMAL(9,4)) as SLOT_HT",
	"CAST(WIDTH AS DECIMAL(9,4)) as SLOT_WIDTH",
	"CAST(DEPTH AS DECIMAL(9,4)) as SLOT_DEPTH",
	"CAST(WT_LIMIT AS DECIMAL(13,4)) as WT_LIMIT",
	"CAST(RACK_TYPE AS INT) as RACK_TYPE",
	"CAST(RT_NAME AS STRING) as RACK_TYPE_NAME",
	"CAST(DAY_OF_WK_NBR AS TINYINT) as DAY_OF_WK_NBR",
	"CAST(LVL AS SMALLINT) as LVL",
	"CAST(TREE_ORDER AS INT) as TREE_ORDER",
	"CAST(TREE AS STRING) as TREE",
	"CAST(NODE_NAME AS STRING) as NODE_NAME",
	"CAST(NODE1 AS STRING) as NODE1",
	"CAST(NODE2 AS STRING) as NODE2",
	"CAST(NODE3 AS STRING) as NODE3",
	"CAST(NODE4 AS STRING) as NODE4",
	"CAST(NODE5 AS STRING) as NODE5",
	"CAST(NODE6 AS STRING) as NODE6",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(SKU_NAME AS INT) as SKU_NBR",
	"CAST(SKU_DESC AS STRING) as SKU_DESC",
	"CAST(WH_TI AS INT) as PALLET_HI",
	"CAST(WH_HI AS INT) as PALLET_TI",
	"CAST(CASE_HT AS DECIMAL(9,4)) as CASE_HT",
	"CAST(CASE_LEN AS DECIMAL(9,4)) as CASE_LEN",
	"CAST(CASE_WID AS DECIMAL(9,4)) as CASE_WID",
	"CAST(CASE_WT AS DECIMAL(9,4)) as CASE_WT",
	"CAST(EACH_HT AS DECIMAL(9,4)) as EACH_HT",
	"CAST(EACH_LEN AS DECIMAL(9,4)) as EACH_LEN",
	"CAST(EACH_WID AS DECIMAL(9,4)) as EACH_WID",
	"CAST(EACH_WT AS DECIMAL(9,4)) as EACH_WT",
	"CAST(SLOTITEM_ID AS INT) as SLOTITEM_ID",
	"CAST(CONST_GRP_ID AS INT) as CONST_GRP_ID",
	"CAST(CNSTR_GRP_NAME AS STRING) as SL_CONST_GRP_DESC",
	"CAST(GRP_GRP_ID AS INT) as GRP_GRP_ID",
	"CAST(SEQ_GRP_ID AS INT) as SEQ_GRP_ID",
	"CAST(SCORE AS DECIMAL(9,4)) as SCORE",
	"CAST(LEGAL_FIT AS TINYINT) as LEGAL_FIT",
	"CAST(LEGAL_FIT_REASON AS INT) as LEGAL_FIT_REASON",
	"CAST(COLOR_ID AS TINYINT) as COLOR_ID",
	"CAST(COLOR_DESC AS STRING) as COLOR_DESC",
	"CAST(CAT_VALUE_TYPE AS STRING) as GROUP_CD",
	"CAST(CAT_DESCRIPTION AS STRING) as GROUP_DESC",
	"CAST(CAT_VALUE_TYPE1 AS STRING) as PKG_CD",
	"CAST(CAT_DESCRIPTION1 AS STRING) as PKG_DESC",
	"CAST(CAT_VALUE_TYPE2 AS STRING) as STATUS_CD",
	"CAST(CAT_DESCRIPTION2 AS STRING) as STATUS_DESC",
	"CAST(DSP_SLOT AS STRING) as DSP_SLOT"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_TREE_WEEK_PRE,'DC_NBR',dcnbr,f'{raw}.SLOT_TREE_WEEK_PRE')
Shortcut_to_SLOT_TREE_WEEK_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SLOT_TREE_WEEK_PRE')
