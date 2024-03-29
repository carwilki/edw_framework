#Code converted on 2023-06-21 18:43:37
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
from logging import getLogger, INFO



def m_WM_Item_Cbo_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Item_Cbo_PRE function")
    
    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ITEM_CBO_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now() #start timestamp of the script


    # COMMAND ----------
    # Variable_declaration_comment
    
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)
    dcnbr = dcnbr.strip()[2:]
    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ITEM_CBO, type SOURCE 
    # COLUMN COUNT: 104

    SQ_Shortcut_to_ITEM_CBO = jdbcOracleConnection(  f"""SELECT
    ITEM_CBO.ITEM_ID,
    ITEM_CBO.COMPANY_ID,
    ITEM_CBO.ITEM_NAME,
    ITEM_CBO.DESCRIPTION,
    ITEM_CBO.PROTECTION_LEVEL_ID,
    ITEM_CBO.COMMODITY_CLASS_ID,
    ITEM_CBO.COMMODITY_CODE_ID,
    ITEM_CBO.PRODUCT_CLASS_ID,
    ITEM_CBO.UN_NUMBER_ID,
    ITEM_CBO.BASE_STORAGE_UOM_ID,
    ITEM_CBO.ITEM_SEASON,
    ITEM_CBO.ITEM_SEASON_YEAR,
    ITEM_CBO.ITEM_STYLE,
    ITEM_CBO.ITEM_STYLE_SFX,
    ITEM_CBO.ITEM_COLOR,
    ITEM_CBO.ITEM_COLOR_SFX,
    ITEM_CBO.ITEM_SECOND_DIM,
    ITEM_CBO.ITEM_QUALITY,
    ITEM_CBO.ITEM_SIZE_DESC,
    ITEM_CBO.ITEM_BAR_CODE,
    ITEM_CBO.ITEM_DESC_SHORT,
    ITEM_CBO.ITEM_UPC_GTIN,
    ITEM_CBO.UNIT_WEIGHT,
    ITEM_CBO.WEIGHT_UOM_ID,
    ITEM_CBO.UNIT_VOLUME,
    ITEM_CBO.VOLUME_UOM_ID,
    ITEM_CBO.UNIT_HEIGHT,
    ITEM_CBO.UNIT_WIDTH,
    ITEM_CBO.UNIT_LENGTH,
    ITEM_CBO.DIMENSION_UOM_ID,
    ITEM_CBO.VARIABLE_WEIGHT,
    ITEM_CBO.DATABASE_QTY_UOM_ID,
    ITEM_CBO.DISPLAY_QTY_UOM_ID,
    ITEM_CBO.STATUS_CODE,
    ITEM_CBO.ITEM_IMAGE_FILENAME,
    ITEM_CBO.AUDIT_CREATED_SOURCE,
    ITEM_CBO.AUDIT_CREATED_SOURCE_TYPE,
    ITEM_CBO.AUDIT_CREATED_DTTM,
    ITEM_CBO.AUDIT_LAST_UPDATED_SOURCE,
    ITEM_CBO.AUDIT_LAST_UPDATED_SOURCE_TYPE,
    ITEM_CBO.AUDIT_LAST_UPDATED_DTTM,
    ITEM_CBO.MARK_FOR_DELETION,
    ITEM_CBO.CATCH_WEIGHT_ITEM,
    ITEM_CBO.COMMODITY_LEVEL_DESC,
    ITEM_CBO.CHANNEL_TYPE_ID,
    ITEM_CBO.COLOR_DESC,
    ITEM_CBO.VERSION,
    ITEM_CBO.ITEM_QUALITY_CODE,
    ITEM_CBO.PROD_TYPE,
    ITEM_CBO.STD_BUNDL_QTY,
    ITEM_CBO.STAB_CODE,
    ITEM_CBO.ITEM_ORIENTATION,
    ITEM_CBO.PROTN_FACTOR,
    ITEM_CBO.CAVITY_LEN,
    ITEM_CBO.CAVITY_WD,
    ITEM_CBO.CAVITY_HT,
    ITEM_CBO.INCREMENTAL_LEN,
    ITEM_CBO.INCREMENTAL_WD,
    ITEM_CBO.INCREMENTAL_HT,
    ITEM_CBO.STACKABLE_ITEM,
    ITEM_CBO.MAX_NEST_NUMBER,
    ITEM_CBO.STCC_CODE_ID,
    ITEM_CBO.SITC_CODE_ID,
    ITEM_CBO.SCHEDULE_B_CODE_ID,
    ITEM_CBO.SOLD_ONLINE,
    ITEM_CBO.SOLD_IN_STORES,
    ITEM_CBO.PICKUP_AT_STORE,
    ITEM_CBO.SHIP_TO_STORE,
    ITEM_CBO.URL,
    ITEM_CBO.IS_RETURNABLE,
    ITEM_CBO.REF_FIELD1,
    ITEM_CBO.REF_FIELD2,
    ITEM_CBO.REF_FIELD3,
    ITEM_CBO.REF_FIELD4,
    ITEM_CBO.REF_FIELD5,
    ITEM_CBO.CUBISCAN_LAST_UPDATED_DTTM,
    ITEM_CBO.IS_EXCHANGEABLE,
    ITEM_CBO.LONG_DESCRIPTION,
    ITEM_CBO.MIN_SHIP_INNER_UOM_ID,
    ITEM_CBO.PRICE_STATUS,
    ITEM_CBO.SELL_THROUGH,
    ITEM_CBO.COMMERCE_ATTRIBUTE1,
    ITEM_CBO.COMMERCE_ATTRIBUTE2,
    ITEM_CBO.COMMERCE_NUM_ATTRIBUTE1,
    ITEM_CBO.COMMERCE_NUM_ATTRIBUTE2,
    ITEM_CBO.COMMERCE_DATE_ATTRIBUTE1,
    ITEM_CBO.BRAND,
    ITEM_CBO.ITEM_DISPOSITION,
    ITEM_CBO.CREATED_DTTM,
    ITEM_CBO.LAST_UPDATED_DTTM,
    ITEM_CBO.REF_FIELD6,
    ITEM_CBO.REF_FIELD7,
    ITEM_CBO.REF_FIELD8,
    ITEM_CBO.REF_FIELD9,
    ITEM_CBO.REF_FIELD10,
    ITEM_CBO.REF_NUM1,
    ITEM_CBO.REF_NUM2,
    ITEM_CBO.REF_NUM3,
    ITEM_CBO.REF_NUM4,
    ITEM_CBO.REF_NUM5,
    ITEM_CBO.SIZE_SEQ,
    ITEM_CBO.COLOR_SEQ,
    ITEM_CBO.RETURNABLE_AT_STORE,
    ITEM_CBO.GIFT_CARD_TYPE
    FROM {source_schema}.ITEM_CBO
    WHERE (trunc(AUDIT_CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(AUDIT_LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 106

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ITEM_CBO_temp = SQ_Shortcut_to_ITEM_CBO.toDF(*["SQ_Shortcut_to_ITEM_CBO___" + col for col in SQ_Shortcut_to_ITEM_CBO.columns])

    EXPTRANS = SQ_Shortcut_to_ITEM_CBO_temp.selectExpr( \
        "SQ_Shortcut_to_ITEM_CBO___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_ID as ITEM_ID", \
        "SQ_Shortcut_to_ITEM_CBO___COMPANY_ID as COMPANY_ID", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_NAME as ITEM_NAME", \
        "SQ_Shortcut_to_ITEM_CBO___DESCRIPTION as DESCRIPTION", \
        "SQ_Shortcut_to_ITEM_CBO___PROTECTION_LEVEL_ID as PROTECTION_LEVEL_ID", \
        "SQ_Shortcut_to_ITEM_CBO___COMMODITY_CLASS_ID as COMMODITY_CLASS_ID", \
        "SQ_Shortcut_to_ITEM_CBO___COMMODITY_CODE_ID as COMMODITY_CODE_ID", \
        "SQ_Shortcut_to_ITEM_CBO___PRODUCT_CLASS_ID as PRODUCT_CLASS_ID", \
        "SQ_Shortcut_to_ITEM_CBO___UN_NUMBER_ID as UN_NUMBER_ID", \
        "SQ_Shortcut_to_ITEM_CBO___BASE_STORAGE_UOM_ID as BASE_STORAGE_UOM_ID", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_SEASON as ITEM_SEASON", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_SEASON_YEAR as ITEM_SEASON_YEAR", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_STYLE as ITEM_STYLE", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_STYLE_SFX as ITEM_STYLE_SFX", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_COLOR as ITEM_COLOR", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_COLOR_SFX as ITEM_COLOR_SFX", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_SECOND_DIM as ITEM_SECOND_DIM", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_QUALITY as ITEM_QUALITY", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_SIZE_DESC as ITEM_SIZE_DESC", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_BAR_CODE as ITEM_BAR_CODE", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_DESC_SHORT as ITEM_DESC_SHORT", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_UPC_GTIN as ITEM_UPC_GTIN", \
        "SQ_Shortcut_to_ITEM_CBO___UNIT_WEIGHT as UNIT_WEIGHT", \
        "SQ_Shortcut_to_ITEM_CBO___WEIGHT_UOM_ID as WEIGHT_UOM_ID", \
        "SQ_Shortcut_to_ITEM_CBO___UNIT_VOLUME as UNIT_VOLUME", \
        "SQ_Shortcut_to_ITEM_CBO___VOLUME_UOM_ID as VOLUME_UOM_ID", \
        "SQ_Shortcut_to_ITEM_CBO___UNIT_HEIGHT as UNIT_HEIGHT", \
        "SQ_Shortcut_to_ITEM_CBO___UNIT_WIDTH as UNIT_WIDTH", \
        "SQ_Shortcut_to_ITEM_CBO___UNIT_LENGTH as UNIT_LENGTH", \
        "SQ_Shortcut_to_ITEM_CBO___DIMENSION_UOM_ID as DIMENSION_UOM_ID", \
        "SQ_Shortcut_to_ITEM_CBO___VARIABLE_WEIGHT as VARIABLE_WEIGHT", \
        "SQ_Shortcut_to_ITEM_CBO___DATABASE_QTY_UOM_ID as DATABASE_QTY_UOM_ID", \
        "SQ_Shortcut_to_ITEM_CBO___DISPLAY_QTY_UOM_ID as DISPLAY_QTY_UOM_ID", \
        "SQ_Shortcut_to_ITEM_CBO___STATUS_CODE as STATUS_CODE", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_IMAGE_FILENAME as ITEM_IMAGE_FILENAME", \
        "SQ_Shortcut_to_ITEM_CBO___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_CBO___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_CBO___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
        "SQ_Shortcut_to_ITEM_CBO___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_CBO___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_CBO___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_ITEM_CBO___MARK_FOR_DELETION as MARK_FOR_DELETION", \
        "SQ_Shortcut_to_ITEM_CBO___CATCH_WEIGHT_ITEM as CATCH_WEIGHT_ITEM", \
        "SQ_Shortcut_to_ITEM_CBO___COMMODITY_LEVEL_DESC as COMMODITY_LEVEL_DESC", \
        "SQ_Shortcut_to_ITEM_CBO___CHANNEL_TYPE_ID as CHANNEL_TYPE_ID", \
        "SQ_Shortcut_to_ITEM_CBO___COLOR_DESC as COLOR_DESC", \
        "SQ_Shortcut_to_ITEM_CBO___VERSION as VERSION", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_QUALITY_CODE as ITEM_QUALITY_CODE", \
        "SQ_Shortcut_to_ITEM_CBO___PROD_TYPE as PROD_TYPE", \
        "SQ_Shortcut_to_ITEM_CBO___STD_BUNDL_QTY as STD_BUNDL_QTY", \
        "SQ_Shortcut_to_ITEM_CBO___STAB_CODE as STAB_CODE", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_ORIENTATION as ITEM_ORIENTATION", \
        "SQ_Shortcut_to_ITEM_CBO___PROTN_FACTOR as PROTN_FACTOR", \
        "SQ_Shortcut_to_ITEM_CBO___CAVITY_LEN as CAVITY_LEN", \
        "SQ_Shortcut_to_ITEM_CBO___CAVITY_WD as CAVITY_WD", \
        "SQ_Shortcut_to_ITEM_CBO___CAVITY_HT as CAVITY_HT", \
        "SQ_Shortcut_to_ITEM_CBO___INCREMENTAL_LEN as INCREMENTAL_LEN", \
        "SQ_Shortcut_to_ITEM_CBO___INCREMENTAL_WD as INCREMENTAL_WD", \
        "SQ_Shortcut_to_ITEM_CBO___INCREMENTAL_HT as INCREMENTAL_HT", \
        "SQ_Shortcut_to_ITEM_CBO___STACKABLE_ITEM as STACKABLE_ITEM", \
        "SQ_Shortcut_to_ITEM_CBO___MAX_NEST_NUMBER as MAX_NEST_NUMBER", \
        "SQ_Shortcut_to_ITEM_CBO___STCC_CODE_ID as STCC_CODE_ID", \
        "SQ_Shortcut_to_ITEM_CBO___SITC_CODE_ID as SITC_CODE_ID", \
        "SQ_Shortcut_to_ITEM_CBO___SCHEDULE_B_CODE_ID as SCHEDULE_B_CODE_ID", \
        "SQ_Shortcut_to_ITEM_CBO___SOLD_ONLINE as SOLD_ONLINE", \
        "SQ_Shortcut_to_ITEM_CBO___SOLD_IN_STORES as SOLD_IN_STORES", \
        "SQ_Shortcut_to_ITEM_CBO___PICKUP_AT_STORE as PICKUP_AT_STORE", \
        "SQ_Shortcut_to_ITEM_CBO___SHIP_TO_STORE as SHIP_TO_STORE", \
        "SQ_Shortcut_to_ITEM_CBO___URL as URL", \
        "SQ_Shortcut_to_ITEM_CBO___IS_RETURNABLE as IS_RETURNABLE", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD1 as REF_FIELD1", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD2 as REF_FIELD2", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD3 as REF_FIELD3", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD4 as REF_FIELD4", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD5 as REF_FIELD5", \
        "SQ_Shortcut_to_ITEM_CBO___CUBISCAN_LAST_UPDATED_DTTM as CUBISCAN_LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_ITEM_CBO___IS_EXCHANGEABLE as IS_EXCHANGEABLE", \
        "SQ_Shortcut_to_ITEM_CBO___LONG_DESCRIPTION as LONG_DESCRIPTION", \
        "SQ_Shortcut_to_ITEM_CBO___MIN_SHIP_INNER_UOM_ID as MIN_SHIP_INNER_UOM_ID", \
        "SQ_Shortcut_to_ITEM_CBO___PRICE_STATUS as PRICE_STATUS", \
        "SQ_Shortcut_to_ITEM_CBO___SELL_THROUGH as SELL_THROUGH", \
        "SQ_Shortcut_to_ITEM_CBO___COMMERCE_ATTRIBUTE1 as COMMERCE_ATTRIBUTE1", \
        "SQ_Shortcut_to_ITEM_CBO___COMMERCE_ATTRIBUTE2 as COMMERCE_ATTRIBUTE2", \
        "SQ_Shortcut_to_ITEM_CBO___COMMERCE_NUM_ATTRIBUTE1 as COMMERCE_NUM_ATTRIBUTE1", \
        "SQ_Shortcut_to_ITEM_CBO___COMMERCE_NUM_ATTRIBUTE2 as COMMERCE_NUM_ATTRIBUTE2", \
        "SQ_Shortcut_to_ITEM_CBO___COMMERCE_DATE_ATTRIBUTE1 as COMMERCE_DATE_ATTRIBUTE1", \
        "SQ_Shortcut_to_ITEM_CBO___BRAND as BRAND", \
        "SQ_Shortcut_to_ITEM_CBO___ITEM_DISPOSITION as ITEM_DISPOSITION", \
        "SQ_Shortcut_to_ITEM_CBO___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_ITEM_CBO___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD6 as REF_FIELD6", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD7 as REF_FIELD7", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD8 as REF_FIELD8", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD9 as REF_FIELD9", \
        "SQ_Shortcut_to_ITEM_CBO___REF_FIELD10 as REF_FIELD10", \
        "SQ_Shortcut_to_ITEM_CBO___REF_NUM1 as REF_NUM1", \
        "SQ_Shortcut_to_ITEM_CBO___REF_NUM2 as REF_NUM2", \
        "SQ_Shortcut_to_ITEM_CBO___REF_NUM3 as REF_NUM3", \
        "SQ_Shortcut_to_ITEM_CBO___REF_NUM4 as REF_NUM4", \
        "SQ_Shortcut_to_ITEM_CBO___REF_NUM5 as REF_NUM5", \
        "SQ_Shortcut_to_ITEM_CBO___SIZE_SEQ as SIZE_SEQ", \
        "SQ_Shortcut_to_ITEM_CBO___COLOR_SEQ as COLOR_SEQ", \
        "SQ_Shortcut_to_ITEM_CBO___RETURNABLE_AT_STORE as RETURNABLE_AT_STORE", \
        "SQ_Shortcut_to_ITEM_CBO___GIFT_CARD_TYPE as GIFT_CARD_TYPE", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ITEM_CBO_PRE, type TARGET 
    # COLUMN COUNT: 106


    Shortcut_to_WM_ITEM_CBO_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(ITEM_ID AS INT) as ITEM_ID",
        "CAST(COMPANY_ID AS INT) as COMPANY_ID",
        "CAST(ITEM_NAME AS STRING) as ITEM_NAME",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(PROTECTION_LEVEL_ID AS INT) as PROTECTION_LEVEL_ID",
        "CAST(COMMODITY_CLASS_ID AS INT) as COMMODITY_CLASS_ID",
        "CAST(COMMODITY_CODE_ID AS BIGINT) as COMMODITY_CODE_ID",
        "CAST(PRODUCT_CLASS_ID AS INT) as PRODUCT_CLASS_ID",
        "CAST(UN_NUMBER_ID AS BIGINT) as UN_NUMBER_ID",
        "CAST(BASE_STORAGE_UOM_ID AS INT) as BASE_STORAGE_UOM_ID",
        "CAST(ITEM_SEASON AS STRING) as ITEM_SEASON",
        "CAST(ITEM_SEASON_YEAR AS STRING) as ITEM_SEASON_YEAR",
        "CAST(ITEM_STYLE AS STRING) as ITEM_STYLE",
        "CAST(ITEM_STYLE_SFX AS STRING) as ITEM_STYLE_SFX",
        "CAST(ITEM_COLOR AS STRING) as ITEM_COLOR",
        "CAST(ITEM_COLOR_SFX AS STRING) as ITEM_COLOR_SFX",
        "CAST(ITEM_SECOND_DIM AS STRING) as ITEM_SECOND_DIM",
        "CAST(ITEM_QUALITY AS STRING) as ITEM_QUALITY",
        "CAST(ITEM_SIZE_DESC AS STRING) as ITEM_SIZE_DESC",
        "CAST(ITEM_BAR_CODE AS STRING) as ITEM_BAR_CODE",
        "CAST(ITEM_DESC_SHORT AS STRING) as ITEM_DESC_SHORT",
        "CAST(ITEM_UPC_GTIN AS STRING) as ITEM_UPC_GTIN",
        "CAST(UNIT_WEIGHT AS DECIMAL(16,4)) as UNIT_WEIGHT",
        "CAST(WEIGHT_UOM_ID AS INT) as WEIGHT_UOM_ID",
        "CAST(UNIT_VOLUME AS DECIMAL(16,4)) as UNIT_VOLUME",
        "CAST(VOLUME_UOM_ID AS INT) as VOLUME_UOM_ID",
        "CAST(UNIT_HEIGHT AS DECIMAL(16,4)) as UNIT_HEIGHT",
        "CAST(UNIT_WIDTH AS DECIMAL(16,4)) as UNIT_WIDTH",
        "CAST(UNIT_LENGTH AS DECIMAL(16,4)) as UNIT_LENGTH",
        "CAST(DIMENSION_UOM_ID AS INT) as DIMENSION_UOM_ID",
        "CAST(VARIABLE_WEIGHT AS TINYINT) as VARIABLE_WEIGHT",
        "CAST(DATABASE_QTY_UOM_ID AS INT) as DATABASE_QTY_UOM_ID",
        "CAST(DISPLAY_QTY_UOM_ID AS INT) as DISPLAY_QTY_UOM_ID",
        "CAST(STATUS_CODE AS TINYINT) as STATUS_CODE",
        "CAST(ITEM_IMAGE_FILENAME AS STRING) as ITEM_IMAGE_FILENAME",
        "CAST(AUDIT_CREATED_SOURCE AS STRING) as AUDIT_CREATED_SOURCE",
        "CAST(AUDIT_CREATED_SOURCE_TYPE AS TINYINT) as AUDIT_CREATED_SOURCE_TYPE",
        "CAST(AUDIT_CREATED_DTTM AS TIMESTAMP) as AUDIT_CREATED_DTTM",
        "CAST(AUDIT_LAST_UPDATED_SOURCE AS STRING) as AUDIT_LAST_UPDATED_SOURCE",
        "CAST(AUDIT_LAST_UPDATED_SOURCE_TYPE AS TINYINT) as AUDIT_LAST_UPDATED_SOURCE_TYPE",
        "CAST(AUDIT_LAST_UPDATED_DTTM AS TIMESTAMP) as AUDIT_LAST_UPDATED_DTTM",
        "CAST(MARK_FOR_DELETION AS TINYINT) as MARK_FOR_DELETION",
        "CAST(CATCH_WEIGHT_ITEM AS STRING) as CATCH_WEIGHT_ITEM",
        "CAST(COMMODITY_LEVEL_DESC AS STRING) as COMMODITY_LEVEL_DESC",
        "CAST(CHANNEL_TYPE_ID AS INT) as CHANNEL_TYPE_ID",
        "CAST(COLOR_DESC AS STRING) as COLOR_DESC",
        "CAST(VERSION AS BIGINT) as VERSION",
        "CAST(ITEM_QUALITY_CODE AS INT) as ITEM_QUALITY_CODE",
        "CAST(PROD_TYPE AS STRING) as PROD_TYPE",
        "CAST(STD_BUNDL_QTY AS DECIMAL(9,2)) as STD_BUNDL_QTY",
        "CAST(STAB_CODE AS STRING) as STAB_CODE",
        "CAST(ITEM_ORIENTATION AS STRING) as ITEM_ORIENTATION",
        "CAST(PROTN_FACTOR AS STRING) as PROTN_FACTOR",
        "CAST(CAVITY_LEN AS DECIMAL(7,2)) as CAVITY_LEN",
        "CAST(CAVITY_WD AS DECIMAL(7,2)) as CAVITY_WD",
        "CAST(CAVITY_HT AS DECIMAL(7,2)) as CAVITY_HT",
        "CAST(INCREMENTAL_LEN AS DECIMAL(16,4)) as INCREMENTAL_LEN",
        "CAST(INCREMENTAL_WD AS DECIMAL(16,4)) as INCREMENTAL_WD",
        "CAST(INCREMENTAL_HT AS DECIMAL(16,4)) as INCREMENTAL_HT",
        "CAST(STACKABLE_ITEM AS STRING) as STACKABLE_ITEM",
        "CAST(MAX_NEST_NUMBER AS SMALLINT) as MAX_NEST_NUMBER",
        "CAST(STCC_CODE_ID AS INT) as STCC_CODE_ID",
        "CAST(SITC_CODE_ID AS INT) as SITC_CODE_ID",
        "CAST(SCHEDULE_B_CODE_ID AS INT) as SCHEDULE_B_CODE_ID",
        "CAST(SOLD_ONLINE AS TINYINT) as SOLD_ONLINE",
        "CAST(SOLD_IN_STORES AS TINYINT) as SOLD_IN_STORES",
        "CAST(PICKUP_AT_STORE AS TINYINT) as PICKUP_AT_STORE",
        "CAST(SHIP_TO_STORE AS TINYINT) as SHIP_TO_STORE",
        "CAST(URL AS STRING) as URL",
        "CAST(IS_RETURNABLE AS TINYINT) as IS_RETURNABLE",
        "CAST(REF_FIELD1 AS STRING) as REF_FIELD1",
        "CAST(REF_FIELD2 AS STRING) as REF_FIELD2",
        "CAST(REF_FIELD3 AS STRING) as REF_FIELD3",
        "CAST(REF_FIELD4 AS STRING) as REF_FIELD4",
        "CAST(REF_FIELD5 AS STRING) as REF_FIELD5",
        "CAST(CUBISCAN_LAST_UPDATED_DTTM AS TIMESTAMP) as CUBISCAN_LAST_UPDATED_DTTM",
        "CAST(IS_EXCHANGEABLE AS TINYINT) as IS_EXCHANGEABLE",
        "CAST(LONG_DESCRIPTION AS STRING) as LONG_DESCRIPTION",
        "CAST(MIN_SHIP_INNER_UOM_ID AS STRING) as MIN_SHIP_INNER_UOM_ID",
        "CAST(PRICE_STATUS AS STRING) as PRICE_STATUS",
        "CAST(SELL_THROUGH AS DECIMAL(13,4)) as SELL_THROUGH",
        "CAST(COMMERCE_ATTRIBUTE1 AS STRING) as COMMERCE_ATTRIBUTE1",
        "CAST(COMMERCE_ATTRIBUTE2 AS STRING) as COMMERCE_ATTRIBUTE2",
        "CAST(COMMERCE_NUM_ATTRIBUTE1 AS DECIMAL(13,4)) as COMMERCE_NUM_ATTRIBUTE1",
        "CAST(COMMERCE_NUM_ATTRIBUTE2 AS DECIMAL(13,4)) as COMMERCE_NUM_ATTRIBUTE2",
        "CAST(COMMERCE_DATE_ATTRIBUTE1 AS TIMESTAMP) as COMMERCE_DATE_ATTRIBUTE1",
        "CAST(BRAND AS STRING) as BRAND",
        "CAST(ITEM_DISPOSITION AS STRING) as ITEM_DISPOSITION",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(REF_FIELD6 AS STRING) as REF_FIELD6",
        "CAST(REF_FIELD7 AS STRING) as REF_FIELD7",
        "CAST(REF_FIELD8 AS STRING) as REF_FIELD8",
        "CAST(REF_FIELD9 AS STRING) as REF_FIELD9",
        "CAST(REF_FIELD10 AS STRING) as REF_FIELD10",
        "CAST(REF_NUM1 AS DECIMAL(13,5)) as REF_NUM1",
        "CAST(REF_NUM2 AS DECIMAL(13,5)) as REF_NUM2",
        "CAST(REF_NUM3 AS DECIMAL(13,5)) as REF_NUM3",
        "CAST(REF_NUM4 AS DECIMAL(13,5)) as REF_NUM4",
        "CAST(REF_NUM5 AS DECIMAL(13,5)) as REF_NUM5",
        "CAST(SIZE_SEQ AS SMALLINT) as SIZE_SEQ",
        "CAST(COLOR_SEQ AS SMALLINT) as COLOR_SEQ",
        "CAST(RETURNABLE_AT_STORE AS TINYINT) as RETURNABLE_AT_STORE",
        "CAST(GIFT_CARD_TYPE AS STRING) as GIFT_CARD_TYPE",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_ITEM_CBO_PRE,"DC_NBR",dcnbr,target_table_name)

    logger.info(
        "Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE is written to the target table - "
        + target_table_name
    )    