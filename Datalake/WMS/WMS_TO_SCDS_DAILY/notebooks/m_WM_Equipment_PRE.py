#Code converted on 2023-06-26 10:02:29
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from logging import getLogger, INFO



def m_WM_Equipment_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Equipment_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_EQUIPMENT_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now() #start timestamp of the script

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)

    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_EQUIPMENT, type SOURCE 
    # COLUMN COUNT: 45

    SQ_Shortcut_to_EQUIPMENT = jdbcOracleConnection( f"""SELECT
    EQUIPMENT.EQUIPMENT_ID,
    EQUIPMENT.TC_COMPANY_ID,
    EQUIPMENT.EQUIPMENT_CODE,
    EQUIPMENT.DESCRIPTION,
    EQUIPMENT.MARK_FOR_DELETION,
    EQUIPMENT.CREATED_SOURCE_TYPE,
    EQUIPMENT.CREATED_SOURCE,
    EQUIPMENT.CREATED_DTTM,
    EQUIPMENT.LAST_UPDATED_SOURCE_TYPE,
    EQUIPMENT.LAST_UPDATED_SOURCE,
    EQUIPMENT.LAST_UPDATED_DTTM,
    EQUIPMENT.SHAPE_TYPE,
    EQUIPMENT.DIM01_VALUE,
    EQUIPMENT.DIM01_STANDARD_UOM,
    EQUIPMENT.DIM02_VALUE,
    EQUIPMENT.DIM02_STANDARD_UOM,
    EQUIPMENT.DIM03_VALUE,
    EQUIPMENT.DIM03_STANDARD_UOM,
    EQUIPMENT.VOLUME_CALC_VALUE,
    EQUIPMENT.VOLUME_CALC_STANDARD_UOM,
    EQUIPMENT.WEIGHT_EMPTY_VALUE,
    EQUIPMENT.WEIGHT_EMPTY_STANDARD_UOM,
    EQUIPMENT.EQUIPMENT_TYPE,
    EQUIPMENT.EQUIP_DW_HEIGHT_VALUE,
    EQUIPMENT.EQUIP_DW_HEIGHT_STANDARD_UOM,
    EQUIPMENT.TRAILER_TYPE,
    EQUIPMENT.OWNERSHIP_TYPE,
    EQUIPMENT.NUMBER_OF_AXLES,
    EQUIPMENT.PER_USAGE_COST,
    EQUIPMENT.PER_USAGE_COST_CURRENCY_CODE,
    EQUIPMENT.PLATED_WEIGHT,
    EQUIPMENT.TAX_BAND_NAME,
    EQUIPMENT.WEIGHT_VALUE,
    EQUIPMENT.WEIGHT_STANDARD_UOM,
    EQUIPMENT.EQUIP_HEIGHT_VALUE,
    EQUIPMENT.EQUIP_HEIGHT_STANDARD_UOM,
    EQUIPMENT.IS_ALLOW_TRAILER_SWAPPING,
    EQUIPMENT.IS_TANDEM_CAPABLE,
    EQUIPMENT.MASTER_EQUIPMENT_ID,
    EQUIPMENT.WEIGHT_EMPTY_SIZE_UOM_ID,
    EQUIPMENT.PLATED_WEIGHT_SIZE_UOM_ID,
    EQUIPMENT.BACKIN_TIME,
    EQUIPMENT.BACKOUT_TIME,
    EQUIPMENT.FLOOR_SPACE_VALUE,
    EQUIPMENT.FLOOR_SPACE_SIZE_UOM_ID
    FROM {source_schema}.EQUIPMENT
    WHERE (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)  AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 47

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_EQUIPMENT_temp = SQ_Shortcut_to_EQUIPMENT.toDF(*["SQ_Shortcut_to_EQUIPMENT___" + col for col in SQ_Shortcut_to_EQUIPMENT.columns])

    EXPTRANS = SQ_Shortcut_to_EQUIPMENT_temp.selectExpr( \
        "SQ_Shortcut_to_EQUIPMENT___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_exp", \
        "SQ_Shortcut_to_EQUIPMENT___EQUIPMENT_ID as EQUIPMENT_ID", \
        "SQ_Shortcut_to_EQUIPMENT___TC_COMPANY_ID as TC_COMPANY_ID", \
        "SQ_Shortcut_to_EQUIPMENT___EQUIPMENT_CODE as EQUIPMENT_CODE", \
        "SQ_Shortcut_to_EQUIPMENT___DESCRIPTION as DESCRIPTION", \
        "SQ_Shortcut_to_EQUIPMENT___MARK_FOR_DELETION as MARK_FOR_DELETION", \
        "SQ_Shortcut_to_EQUIPMENT___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_EQUIPMENT___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_EQUIPMENT___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_EQUIPMENT___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_EQUIPMENT___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_EQUIPMENT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_EQUIPMENT___SHAPE_TYPE as SHAPE_TYPE", \
        "SQ_Shortcut_to_EQUIPMENT___DIM01_VALUE as DIM01_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___DIM01_STANDARD_UOM as DIM01_STANDARD_UOM", \
        "SQ_Shortcut_to_EQUIPMENT___DIM02_VALUE as DIM02_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___DIM02_STANDARD_UOM as DIM02_STANDARD_UOM", \
        "SQ_Shortcut_to_EQUIPMENT___DIM03_VALUE as DIM03_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___DIM03_STANDARD_UOM as DIM03_STANDARD_UOM", \
        "SQ_Shortcut_to_EQUIPMENT___VOLUME_CALC_VALUE as VOLUME_CALC_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___VOLUME_CALC_STANDARD_UOM as VOLUME_CALC_STANDARD_UOM", \
        "SQ_Shortcut_to_EQUIPMENT___WEIGHT_EMPTY_VALUE as WEIGHT_EMPTY_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___WEIGHT_EMPTY_STANDARD_UOM as WEIGHT_EMPTY_STANDARD_UOM", \
        "SQ_Shortcut_to_EQUIPMENT___EQUIPMENT_TYPE as EQUIPMENT_TYPE", \
        "SQ_Shortcut_to_EQUIPMENT___EQUIP_DW_HEIGHT_VALUE as EQUIP_DW_HEIGHT_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___EQUIP_DW_HEIGHT_STANDARD_UOM as EQUIP_DW_HEIGHT_STANDARD_UOM", \
        "SQ_Shortcut_to_EQUIPMENT___TRAILER_TYPE as TRAILER_TYPE", \
        "SQ_Shortcut_to_EQUIPMENT___OWNERSHIP_TYPE as OWNERSHIP_TYPE", \
        "SQ_Shortcut_to_EQUIPMENT___NUMBER_OF_AXLES as NUMBER_OF_AXLES", \
        "SQ_Shortcut_to_EQUIPMENT___PER_USAGE_COST as PER_USAGE_COST", \
        "SQ_Shortcut_to_EQUIPMENT___PER_USAGE_COST_CURRENCY_CODE as PER_USAGE_COST_CURRENCY_CODE", \
        "SQ_Shortcut_to_EQUIPMENT___PLATED_WEIGHT as PLATED_WEIGHT", \
        "SQ_Shortcut_to_EQUIPMENT___TAX_BAND_NAME as TAX_BAND_NAME", \
        "SQ_Shortcut_to_EQUIPMENT___WEIGHT_VALUE as WEIGHT_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___WEIGHT_STANDARD_UOM as WEIGHT_STANDARD_UOM", \
        "SQ_Shortcut_to_EQUIPMENT___EQUIP_HEIGHT_VALUE as EQUIP_HEIGHT_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___EQUIP_HEIGHT_STANDARD_UOM as EQUIP_HEIGHT_STANDARD_UOM", \
        "SQ_Shortcut_to_EQUIPMENT___IS_ALLOW_TRAILER_SWAPPING as IS_ALLOW_TRAILER_SWAPPING", \
        "SQ_Shortcut_to_EQUIPMENT___IS_TANDEM_CAPABLE as IS_TANDEM_CAPABLE", \
        "SQ_Shortcut_to_EQUIPMENT___MASTER_EQUIPMENT_ID as MASTER_EQUIPMENT_ID", \
        "SQ_Shortcut_to_EQUIPMENT___WEIGHT_EMPTY_SIZE_UOM_ID as WEIGHT_EMPTY_SIZE_UOM_ID", \
        "SQ_Shortcut_to_EQUIPMENT___PLATED_WEIGHT_SIZE_UOM_ID as PLATED_WEIGHT_SIZE_UOM_ID", \
        "SQ_Shortcut_to_EQUIPMENT___BACKIN_TIME as BACKIN_TIME", \
        "SQ_Shortcut_to_EQUIPMENT___BACKOUT_TIME as BACKOUT_TIME", \
        "SQ_Shortcut_to_EQUIPMENT___FLOOR_SPACE_VALUE as FLOOR_SPACE_VALUE", \
        "SQ_Shortcut_to_EQUIPMENT___FLOOR_SPACE_SIZE_UOM_ID as FLOOR_SPACE_SIZE_UOM_ID", \
        "CURRENT_TIMESTAMP() as LOADTSTMP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_EQUIPMENT_PRE, type TARGET 
    # COLUMN COUNT: 47


    Shortcut_to_WM_EQUIPMENT_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_exp AS SMALLINT) as DC_NBR",
        "CAST(EQUIPMENT_ID AS BIGINT) as EQUIPMENT_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(EQUIPMENT_CODE AS STRING) as EQUIPMENT_CODE",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(MARK_FOR_DELETION AS SMALLINT) as MARK_FOR_DELETION",
        "CAST(CREATED_SOURCE_TYPE AS SMALLINT) as CREATED_SOURCE_TYPE",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS SMALLINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(SHAPE_TYPE AS SMALLINT) as SHAPE_TYPE",
        "CAST(DIM01_VALUE AS DECIMAL(16,4)) as DIM01_VALUE",
        "CAST(DIM01_STANDARD_UOM AS INT) as DIM01_STANDARD_UOM",
        "CAST(DIM02_VALUE AS DECIMAL(16,4)) as DIM02_VALUE",
        "CAST(DIM02_STANDARD_UOM AS INT) as DIM02_STANDARD_UOM",
        "CAST(DIM03_VALUE AS DECIMAL(16,4)) as DIM03_VALUE",
        "CAST(DIM03_STANDARD_UOM AS INT) as DIM03_STANDARD_UOM",
        "CAST(VOLUME_CALC_VALUE AS DECIMAL(16,4)) as VOLUME_CALC_VALUE",
        "CAST(VOLUME_CALC_STANDARD_UOM AS INT) as VOLUME_CALC_STANDARD_UOM",
        "CAST(WEIGHT_EMPTY_VALUE AS DECIMAL(16,4)) as WEIGHT_EMPTY_VALUE",
        "CAST(WEIGHT_EMPTY_STANDARD_UOM AS INT) as WEIGHT_EMPTY_STANDARD_UOM",
        "CAST(EQUIPMENT_TYPE AS SMALLINT) as EQUIPMENT_TYPE",
        "CAST(EQUIP_DW_HEIGHT_VALUE AS DECIMAL(16,4)) as EQUIP_DW_HEIGHT_VALUE",
        "CAST(EQUIP_DW_HEIGHT_STANDARD_UOM AS INT) as EQUIP_DW_HEIGHT_STANDARD_UOM",
        "CAST(TRAILER_TYPE AS TINYINT) as TRAILER_TYPE",
        "CAST(OWNERSHIP_TYPE AS SMALLINT) as OWNERSHIP_TYPE",
        "CAST(NUMBER_OF_AXLES AS SMALLINT) as NUMBER_OF_AXLES",
        "CAST(PER_USAGE_COST AS DECIMAL(13,2)) as PER_USAGE_COST",
        "CAST(PER_USAGE_COST_CURRENCY_CODE AS STRING) as PER_USAGE_COST_CURRENCY_CODE",
        "CAST(PLATED_WEIGHT AS DECIMAL(13,2)) as PLATED_WEIGHT",
        "CAST(TAX_BAND_NAME AS STRING) as TAX_BAND_NAME",
        "CAST(WEIGHT_VALUE AS DECIMAL(13,4)) as WEIGHT_VALUE",
        "CAST(WEIGHT_STANDARD_UOM AS INT) as WEIGHT_STANDARD_UOM",
        "CAST(EQUIP_HEIGHT_VALUE AS INT) as EQUIP_HEIGHT_VALUE",
        "CAST(EQUIP_HEIGHT_STANDARD_UOM AS INT) as EQUIP_HEIGHT_STANDARD_UOM",
        "CAST(IS_ALLOW_TRAILER_SWAPPING AS SMALLINT) as IS_ALLOW_TRAILER_SWAPPING",
        "CAST(IS_TANDEM_CAPABLE AS TINYINT) as IS_TANDEM_CAPABLE",
        "CAST(MASTER_EQUIPMENT_ID AS BIGINT) as MASTER_EQUIPMENT_ID",
        "CAST(WEIGHT_EMPTY_SIZE_UOM_ID AS INT) as WEIGHT_EMPTY_SIZE_UOM_ID",
        "CAST(PLATED_WEIGHT_SIZE_UOM_ID AS INT) as PLATED_WEIGHT_SIZE_UOM_ID",
        "CAST(BACKIN_TIME AS SMALLINT) as BACKIN_TIME",
        "CAST(BACKOUT_TIME AS SMALLINT) as BACKOUT_TIME",
        "CAST(FLOOR_SPACE_VALUE AS BIGINT) as FLOOR_SPACE_VALUE",
        "CAST(FLOOR_SPACE_SIZE_UOM_ID AS INT) as FLOOR_SPACE_SIZE_UOM_ID",
        "CAST(LOADTSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )

    overwriteDeltaPartition(Shortcut_to_WM_EQUIPMENT_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_EQUIPMENT_PRE is written to the target table - "
        + target_table_name
    )    