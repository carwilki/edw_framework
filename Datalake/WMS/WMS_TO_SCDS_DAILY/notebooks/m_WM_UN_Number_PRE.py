#Code converted on 2023-06-22 21:03:30
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
from Datalake.utils.logger import *



def m_WM_UN_Number_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_UN_Number_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_UN_NUMBER_PRE"
    schemaName = raw
    source_schema = "WMSMIS"

    
    target_table_name = schemaName + "." + tableName
    refine_table_name = tableName[:-4]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + Prev_Run_Dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    UN_NUMBER.UN_NUMBER_ID,
                    UN_NUMBER.TC_COMPANY_ID,
                    UN_NUMBER.UN_NUMBER_VALUE,
                    UN_NUMBER.UN_NUMBER_DESCRIPTION,
                    UN_NUMBER.UNN_CLASS_DIV,
                    UN_NUMBER.UNN_SUB_RISK,
                    UN_NUMBER.UNN_HAZARD_LABEL,
                    UN_NUMBER.UNN_PG,
                    UN_NUMBER.UNN_SP,
                    UN_NUMBER.UNN_AIR_BOTH_INST_LTD,
                    UN_NUMBER.UNN_AIR_BOTH_QTY_LTD,
                    UN_NUMBER.UNN_AIR_BOTH_INST,
                    UN_NUMBER.UNN_AIR_BOTH_QTY,
                    UN_NUMBER.UNN_AIR_CARGO_INST,
                    UN_NUMBER.UNN_AIR_CARGO_QTY,
                    UN_NUMBER.UN_NUMBER,
                    UN_NUMBER.RQ_FIELD_1,
                    UN_NUMBER.RQ_FIELD_2,
                    UN_NUMBER.RQ_FIELD_3,
                    UN_NUMBER.RQ_FIELD_4,
                    UN_NUMBER.RQ_FIELD_5,
                    UN_NUMBER.HZC_FIELD_1,
                    UN_NUMBER.HZC_FIELD_2,
                    UN_NUMBER.HZC_FIELD_3,
                    UN_NUMBER.HZC_FIELD_4,
                    UN_NUMBER.HZC_FIELD_5,
                    UN_NUMBER.MARK_FOR_DELETION,
                    UN_NUMBER.CREATED_SOURCE_TYPE,
                    UN_NUMBER.CREATED_SOURCE,
                    UN_NUMBER.CREATED_DTTM,
                    UN_NUMBER.LAST_UPDATED_SOURCE_TYPE,
                    UN_NUMBER.LAST_UPDATED_SOURCE,
                    UN_NUMBER.LAST_UPDATED_DTTM,
                    UN_NUMBER.UN_NUMBER_VERSION,
                    UN_NUMBER.HAZMAT_UOM,
                    UN_NUMBER.MAX_HAZMAT_QTY,
                    UN_NUMBER.REGULATION_SET,
                    UN_NUMBER.TRANS_MODE,
                    UN_NUMBER.REPORTABLE_QTY,
                    UN_NUMBER.TECHNICAL_NAME,
                    UN_NUMBER.ADDL_DESC,
                    UN_NUMBER.PHONE_NUMBER,
                    UN_NUMBER.CONTACT_NAME,
                    UN_NUMBER.IS_ACCESSIBLE,
                    UN_NUMBER.RESIDUE_INDICATOR_CODE,
                    UN_NUMBER.HAZMAT_CLASS_QUAL,
                    UN_NUMBER.NOS_INDICATOR_CODE,
                    UN_NUMBER.HAZMAT_MATERIAL_QUAL,
                    UN_NUMBER.FLASH_POINT_TEMPERATURE,
                    UN_NUMBER.FLASH_POINT_TEMPERATURE_UOM,
                    UN_NUMBER.HAZARD_ZONE_CODE,
                    UN_NUMBER.RADIOACTIVE_QTY_CALC_FACTOR,
                    UN_NUMBER.RADIOACTIVE_QUANTITY_UOM,
                    UN_NUMBER.EPAWASTESTREAM_NUMBER,
                    UN_NUMBER.WASTE_CHARACTERISTIC_CODE,
                    UN_NUMBER.NET_EXPLOSIVE_QUANTITY_FACTOR,
                    UN_NUMBER.HAZMAT_EXEMPTION_REF_NUMBR,
                    UN_NUMBER.RQ_FIELD_6,
                    UN_NUMBER.RQ_FIELD_7,
                    UN_NUMBER.RQ_FIELD_8,
                    UN_NUMBER.RQ_FIELD_9,
                    UN_NUMBER.RQ_FIELD_10,
                    UN_NUMBER.RQ_FIELD_11,
                    UN_NUMBER.RQ_FIELD_12,
                    UN_NUMBER.RQ_FIELD_13,
                    UN_NUMBER.RQ_FIELD_14,
                    UN_NUMBER.RQ_FIELD_15,
                    UN_NUMBER.RQ_FIELD_16,
                    UN_NUMBER.RQ_FIELD_17,
                    UN_NUMBER.RQ_FIELD_18,
                    UN_NUMBER.RQ_FIELD_19,
                    UN_NUMBER.RQ_FIELD_20
                FROM {source_schema}.UN_NUMBER
                WHERE  (TRUNC(CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC(LAST_UPDATED_DTTM) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
            

    SQ_Shortcut_to_UN_NUMBER = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_UN_NUMBER is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 74
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_UN_NUMBER_temp = SQ_Shortcut_to_UN_NUMBER.toDF(*["SQ_Shortcut_to_UN_NUMBER___" + col for col in SQ_Shortcut_to_UN_NUMBER.columns])
    
    EXPTRANS = SQ_Shortcut_to_UN_NUMBER_temp.selectExpr( 
        "SQ_Shortcut_to_UN_NUMBER___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_UN_NUMBER___UN_NUMBER_ID as UN_NUMBER_ID", 
        "SQ_Shortcut_to_UN_NUMBER___TC_COMPANY_ID as TC_COMPANY_ID", 
        "SQ_Shortcut_to_UN_NUMBER___UN_NUMBER_VALUE as UN_NUMBER_VALUE", 
        "SQ_Shortcut_to_UN_NUMBER___UN_NUMBER_DESCRIPTION as UN_NUMBER_DESCRIPTION", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_CLASS_DIV as UNN_CLASS_DIV", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_SUB_RISK as UNN_SUB_RISK", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_HAZARD_LABEL as UNN_HAZARD_LABEL", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_PG as UNN_PG", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_SP as UNN_SP", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_AIR_BOTH_INST_LTD as UNN_AIR_BOTH_INST_LTD", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_AIR_BOTH_QTY_LTD as UNN_AIR_BOTH_QTY_LTD", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_AIR_BOTH_INST as UNN_AIR_BOTH_INST", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_AIR_BOTH_QTY as UNN_AIR_BOTH_QTY", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_AIR_CARGO_INST as UNN_AIR_CARGO_INST", 
        "SQ_Shortcut_to_UN_NUMBER___UNN_AIR_CARGO_QTY as UNN_AIR_CARGO_QTY", 
        "SQ_Shortcut_to_UN_NUMBER___UN_NUMBER as UN_NUMBER", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_1 as RQ_FIELD_1", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_2 as RQ_FIELD_2", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_3 as RQ_FIELD_3", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_4 as RQ_FIELD_4", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_5 as RQ_FIELD_5", 
        "SQ_Shortcut_to_UN_NUMBER___HZC_FIELD_1 as HZC_FIELD_1", 
        "SQ_Shortcut_to_UN_NUMBER___HZC_FIELD_2 as HZC_FIELD_2", 
        "SQ_Shortcut_to_UN_NUMBER___HZC_FIELD_3 as HZC_FIELD_3", 
        "SQ_Shortcut_to_UN_NUMBER___HZC_FIELD_4 as HZC_FIELD_4", 
        "SQ_Shortcut_to_UN_NUMBER___HZC_FIELD_5 as HZC_FIELD_5", 
        "SQ_Shortcut_to_UN_NUMBER___MARK_FOR_DELETION as MARK_FOR_DELETION", 
        "SQ_Shortcut_to_UN_NUMBER___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_UN_NUMBER___CREATED_SOURCE as CREATED_SOURCE", 
        "SQ_Shortcut_to_UN_NUMBER___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_UN_NUMBER___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_UN_NUMBER___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
        "SQ_Shortcut_to_UN_NUMBER___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "SQ_Shortcut_to_UN_NUMBER___UN_NUMBER_VERSION as UN_NUMBER_VERSION", 
        "SQ_Shortcut_to_UN_NUMBER___HAZMAT_UOM as HAZMAT_UOM", 
        "SQ_Shortcut_to_UN_NUMBER___MAX_HAZMAT_QTY as MAX_HAZMAT_QTY", 
        "SQ_Shortcut_to_UN_NUMBER___REGULATION_SET as REGULATION_SET", 
        "SQ_Shortcut_to_UN_NUMBER___TRANS_MODE as TRANS_MODE", 
        "SQ_Shortcut_to_UN_NUMBER___REPORTABLE_QTY as REPORTABLE_QTY", 
        "SQ_Shortcut_to_UN_NUMBER___TECHNICAL_NAME as TECHNICAL_NAME", 
        "SQ_Shortcut_to_UN_NUMBER___ADDL_DESC as ADDL_DESC", 
        "SQ_Shortcut_to_UN_NUMBER___PHONE_NUMBER as PHONE_NUMBER", 
        "SQ_Shortcut_to_UN_NUMBER___CONTACT_NAME as CONTACT_NAME", 
        "SQ_Shortcut_to_UN_NUMBER___IS_ACCESSIBLE as IS_ACCESSIBLE", 
        "SQ_Shortcut_to_UN_NUMBER___RESIDUE_INDICATOR_CODE as RESIDUE_INDICATOR_CODE", 
        "SQ_Shortcut_to_UN_NUMBER___HAZMAT_CLASS_QUAL as HAZMAT_CLASS_QUAL", 
        "SQ_Shortcut_to_UN_NUMBER___NOS_INDICATOR_CODE as NOS_INDICATOR_CODE", 
        "SQ_Shortcut_to_UN_NUMBER___HAZMAT_MATERIAL_QUAL as HAZMAT_MATERIAL_QUAL", 
        "SQ_Shortcut_to_UN_NUMBER___FLASH_POINT_TEMPERATURE as FLASH_POINT_TEMPERATURE", 
        "SQ_Shortcut_to_UN_NUMBER___FLASH_POINT_TEMPERATURE_UOM as FLASH_POINT_TEMPERATURE_UOM", 
        "SQ_Shortcut_to_UN_NUMBER___HAZARD_ZONE_CODE as HAZARD_ZONE_CODE", 
        "SQ_Shortcut_to_UN_NUMBER___RADIOACTIVE_QTY_CALC_FACTOR as RADIOACTIVE_QTY_CALC_FACTOR", 
        "SQ_Shortcut_to_UN_NUMBER___RADIOACTIVE_QUANTITY_UOM as RADIOACTIVE_QUANTITY_UOM", 
        "SQ_Shortcut_to_UN_NUMBER___EPAWASTESTREAM_NUMBER as EPAWASTESTREAM_NUMBER", 
        "SQ_Shortcut_to_UN_NUMBER___WASTE_CHARACTERISTIC_CODE as WASTE_CHARACTERISTIC_CODE", 
        "SQ_Shortcut_to_UN_NUMBER___NET_EXPLOSIVE_QUANTITY_FACTOR as NET_EXPLOSIVE_QUANTITY_FACTOR", 
        "SQ_Shortcut_to_UN_NUMBER___HAZMAT_EXEMPTION_REF_NUMBR as HAZMAT_EXEMPTION_REF_NUMBR", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_6 as RQ_FIELD_6", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_7 as RQ_FIELD_7", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_8 as RQ_FIELD_8", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_9 as RQ_FIELD_9", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_10 as RQ_FIELD_10", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_11 as RQ_FIELD_11", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_12 as RQ_FIELD_12", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_13 as RQ_FIELD_13", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_14 as RQ_FIELD_14", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_15 as RQ_FIELD_15", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_16 as RQ_FIELD_16", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_17 as RQ_FIELD_17", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_18 as RQ_FIELD_18", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_19 as RQ_FIELD_19", 
        "SQ_Shortcut_to_UN_NUMBER___RQ_FIELD_20 as RQ_FIELD_20", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP" 
    )
    
    
    # Processing node Shortcut_to_WM_UN_NUMBER_PRE, type TARGET 
    # COLUMN COUNT: 74
    
    
    Shortcut_to_WM_UN_NUMBER_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(UN_NUMBER_ID AS BIGINT) as UN_NUMBER_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(UN_NUMBER_VALUE AS STRING) as UN_NUMBER_VALUE",
        "CAST(UN_NUMBER_DESCRIPTION AS STRING) as UN_NUMBER_DESCRIPTION",
        "CAST(UNN_CLASS_DIV AS STRING) as UNN_CLASS_DIV",
        "CAST(UNN_SUB_RISK AS STRING) as UNN_SUB_RISK",
        "CAST(UNN_HAZARD_LABEL AS STRING) as UNN_HAZARD_LABEL",
        "CAST(UNN_PG AS STRING) as UNN_PG",
        "CAST(UNN_SP AS STRING) as UNN_SP",
        "CAST(UNN_AIR_BOTH_INST_LTD AS STRING) as UNN_AIR_BOTH_INST_LTD",
        "CAST(UNN_AIR_BOTH_QTY_LTD AS STRING) as UNN_AIR_BOTH_QTY_LTD",
        "CAST(UNN_AIR_BOTH_INST AS STRING) as UNN_AIR_BOTH_INST",
        "CAST(UNN_AIR_BOTH_QTY AS STRING) as UNN_AIR_BOTH_QTY",
        "CAST(UNN_AIR_CARGO_INST AS STRING) as UNN_AIR_CARGO_INST",
        "CAST(UNN_AIR_CARGO_QTY AS STRING) as UNN_AIR_CARGO_QTY",
        "CAST(UN_NUMBER AS BIGINT) as UN_NUMBER",
        "CAST(RQ_FIELD_1 AS STRING) as RQ_FIELD_1",
        "CAST(RQ_FIELD_2 AS STRING) as RQ_FIELD_2",
        "CAST(RQ_FIELD_3 AS STRING) as RQ_FIELD_3",
        "CAST(RQ_FIELD_4 AS STRING) as RQ_FIELD_4",
        "CAST(RQ_FIELD_5 AS STRING) as RQ_FIELD_5",
        "CAST(HZC_FIELD_1 AS STRING) as HZC_FIELD_1",
        "CAST(HZC_FIELD_2 AS STRING) as HZC_FIELD_2",
        "CAST(HZC_FIELD_3 AS STRING) as HZC_FIELD_3",
        "CAST(HZC_FIELD_4 AS STRING) as HZC_FIELD_4",
        "CAST(HZC_FIELD_5 AS STRING) as HZC_FIELD_5",
        "CAST(MARK_FOR_DELETION AS TINYINT) as MARK_FOR_DELETION",
        "CAST(CREATED_SOURCE_TYPE AS TINYINT) as CREATED_SOURCE_TYPE",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(UN_NUMBER_VERSION AS STRING) as UN_NUMBER_VERSION",
        "CAST(HAZMAT_UOM AS INT) as HAZMAT_UOM",
        "CAST(MAX_HAZMAT_QTY AS DECIMAL(13,4)) as MAX_HAZMAT_QTY",
        "CAST(REGULATION_SET AS STRING) as REGULATION_SET",
        "CAST(TRANS_MODE AS STRING) as TRANS_MODE",
        "CAST(REPORTABLE_QTY AS TINYINT) as REPORTABLE_QTY",
        "CAST(TECHNICAL_NAME AS STRING) as TECHNICAL_NAME",
        "CAST(ADDL_DESC AS STRING) as ADDL_DESC",
        "CAST(PHONE_NUMBER AS STRING) as PHONE_NUMBER",
        "CAST(CONTACT_NAME AS STRING) as CONTACT_NAME",
        "CAST(IS_ACCESSIBLE AS TINYINT) as IS_ACCESSIBLE",
        "CAST(RESIDUE_INDICATOR_CODE AS STRING) as RESIDUE_INDICATOR_CODE",
        "CAST(HAZMAT_CLASS_QUAL AS STRING) as HAZMAT_CLASS_QUAL",
        "CAST(NOS_INDICATOR_CODE AS TINYINT) as NOS_INDICATOR_CODE",
        "CAST(HAZMAT_MATERIAL_QUAL AS STRING) as HAZMAT_MATERIAL_QUAL",
        "CAST(FLASH_POINT_TEMPERATURE AS DECIMAL(13,4)) as FLASH_POINT_TEMPERATURE",
        "CAST(FLASH_POINT_TEMPERATURE_UOM AS STRING) as FLASH_POINT_TEMPERATURE_UOM",
        "CAST(HAZARD_ZONE_CODE AS STRING) as HAZARD_ZONE_CODE",
        "CAST(RADIOACTIVE_QTY_CALC_FACTOR AS DECIMAL(13,4)) as RADIOACTIVE_QTY_CALC_FACTOR",
        "CAST(RADIOACTIVE_QUANTITY_UOM AS STRING) as RADIOACTIVE_QUANTITY_UOM",
        "CAST(EPAWASTESTREAM_NUMBER AS STRING) as EPAWASTESTREAM_NUMBER",
        "CAST(WASTE_CHARACTERISTIC_CODE AS STRING) as WASTE_CHARACTERISTIC_CODE",
        "CAST(NET_EXPLOSIVE_QUANTITY_FACTOR AS DECIMAL(13,4)) as NET_EXPLOSIVE_QUANTITY_FACTOR",
        "CAST(HAZMAT_EXEMPTION_REF_NUMBR AS STRING) as HAZMAT_EXEMPTION_REF_NUMBR",
        "CAST(RQ_FIELD_6 AS STRING) as RQ_FIELD_6",
        "CAST(RQ_FIELD_7 AS STRING) as RQ_FIELD_7",
        "CAST(RQ_FIELD_8 AS STRING) as RQ_FIELD_8",
        "CAST(RQ_FIELD_9 AS STRING) as RQ_FIELD_9",
        "CAST(RQ_FIELD_10 AS STRING) as RQ_FIELD_10",
        "CAST(RQ_FIELD_11 AS STRING) as RQ_FIELD_11",
        "CAST(RQ_FIELD_12 AS STRING) as RQ_FIELD_12",
        "CAST(RQ_FIELD_13 AS STRING) as RQ_FIELD_13",
        "CAST(RQ_FIELD_14 AS STRING) as RQ_FIELD_14",
        "CAST(RQ_FIELD_15 AS STRING) as RQ_FIELD_15",
        "CAST(RQ_FIELD_16 AS STRING) as RQ_FIELD_16",
        "CAST(RQ_FIELD_17 AS STRING) as RQ_FIELD_17",
        "CAST(RQ_FIELD_18 AS STRING) as RQ_FIELD_18",
        "CAST(RQ_FIELD_19 AS STRING) as RQ_FIELD_19",
        "CAST(RQ_FIELD_20 AS STRING) as RQ_FIELD_20",
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )
        
    overwriteDeltaPartition(Shortcut_to_WM_UN_NUMBER_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_UN_NUMBER_PRE is written to the target table - " + target_table_name)
