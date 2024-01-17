# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE  refine.WM_ITEM_FACILITY_SLOTTING_dup
# MAGIC ( LOCATION_ID INT not null
# MAGIC
# MAGIC , WM_ITEM_FACILITY_MAPPING_ID                INT                not null
# MAGIC
# MAGIC , WM_ITEM_ID                                 INT 
# MAGIC
# MAGIC , WM_ITEM_NAME                               STRING 
# MAGIC
# MAGIC , WM_WHSE_CD                                 STRING 
# MAGIC
# MAGIC , WM_SLOTTING_GROUP                          STRING 
# MAGIC
# MAGIC , DISC_TRANS_TSTMP                           TIMESTAMP 
# MAGIC
# MAGIC , EX_RECEIPT_TSTMP                           TIMESTAMP 
# MAGIC
# MAGIC , UNIT_MEASURE                               BIGINT 
# MAGIC
# MAGIC , EACH_PER_BIN                               BIGINT 
# MAGIC
# MAGIC , NBR_UNITS_PER_BIN                          BIGINT 
# MAGIC
# MAGIC , INN_PER_CS                                 BIGINT 
# MAGIC
# MAGIC , MAX_SLOTS                                  BIGINT 
# MAGIC
# MAGIC , MAX_PALLET_STACKING                        BIGINT 
# MAGIC
# MAGIC , MAX_STACKING                               BIGINT 
# MAGIC
# MAGIC , MAX_LANES                                  BIGINT 
# MAGIC
# MAGIC , WM_RESERVE_RACK_TYPE_ID                    BIGINT 
# MAGIC
# MAGIC , AFRAME_HT                                  DECIMAL(9,4) 
# MAGIC
# MAGIC , AFRAME_LEN                                 DECIMAL(9,4) 
# MAGIC
# MAGIC , AFRAME_WID                                 DECIMAL(9,4) 
# MAGIC
# MAGIC , AFRAME_WT                                  DECIMAL(9,4) 
# MAGIC
# MAGIC , AFRAME_ALLOW_FLAG                          TINYINT 
# MAGIC
# MAGIC , ALLOW_SLU                                  BIGINT 
# MAGIC
# MAGIC , ALLOW_SU                                   BIGINT 
# MAGIC
# MAGIC , ALLOW_RESERVE                              BIGINT 
# MAGIC
# MAGIC , PALPAT_RESERVE                             SMALLINT 
# MAGIC
# MAGIC , THREE_D_CALC_DONE                          TINYINT 
# MAGIC
# MAGIC , PROP_BORROWING_OBJECT                      BIGINT 
# MAGIC
# MAGIC , PROP_BORROWING_SPECIFIC                    DECIMAL(19,0) 
# MAGIC
# MAGIC , HEIGHT_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , LENGTH_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , WIDTH_CAN_BORROW_FLAG                      TINYINT 
# MAGIC
# MAGIC , WEIGHT_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , VEND_PACK_CAN_BORROW_FLAG                  TINYINT 
# MAGIC
# MAGIC , INNER_PACK_CAN_BORROW_FLAG                 TINYINT 
# MAGIC
# MAGIC , TI_CAN_BORROW_FLAG                         TINYINT 
# MAGIC
# MAGIC , HI_CAN_BORROW_FLAG                         TINYINT 
# MAGIC
# MAGIC , QTY_PER_GRAB_CAN_BORROW_FLAG               TINYINT 
# MAGIC
# MAGIC , HAND_ATTR_CAN_BORROW_FLAG                  TINYINT 
# MAGIC
# MAGIC , FAM_GRP_CAN_BORROW_FLAG                    TINYINT 
# MAGIC
# MAGIC , COMMODITY_CAN_BORROW_FLAG                  TINYINT 
# MAGIC
# MAGIC , CRUSHABILITY_CAN_BORROW_FLAG               TINYINT 
# MAGIC
# MAGIC , HAZARD_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , VEND_CODE_CAN_BORROW_FLAG                  TINYINT 
# MAGIC
# MAGIC , HEIGHT_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , LENGTH_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , WIDTH_BORROWED_FLAG                        TINYINT 
# MAGIC
# MAGIC , WEIGHT_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , VEND_PACK_BORROWED_FLAG                    TINYINT 
# MAGIC
# MAGIC , INNER_PACK_BORROWED_FLAG                   TINYINT 
# MAGIC
# MAGIC , TI_BORROWED_FLAG                           TINYINT 
# MAGIC
# MAGIC , HI_BORROWED_FLAG                           TINYINT 
# MAGIC
# MAGIC , QTY_PER_GRAB_BORROWED_FLAG                 TINYINT 
# MAGIC
# MAGIC , HAND_ATTR_BORROWED_FLAG                    TINYINT 
# MAGIC
# MAGIC , FAM_GRP_BORROWED_FLAG                      TINYINT 
# MAGIC
# MAGIC , COMMODITY_BORROWED_FLAG                    TINYINT 
# MAGIC
# MAGIC , CRUSHABILITY_BORROWED_FLAG                 TINYINT 
# MAGIC
# MAGIC , HAZARD_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , VEND_CODE_BORROWED_FLAG                    TINYINT 
# MAGIC
# MAGIC , ITEM_NUM_1                                 DECIMAL(13,4) 
# MAGIC
# MAGIC , ITEM_NUM_2                                 DECIMAL(13,4) 
# MAGIC
# MAGIC , ITEM_NUM_3                                 DECIMAL(13,4) 
# MAGIC
# MAGIC , ITEM_NUM_4                                 DECIMAL(13,4) 
# MAGIC
# MAGIC , ITEM_NUM_5                                 DECIMAL(13,4) 
# MAGIC
# MAGIC , ITEM_CHAR_1                                STRING 
# MAGIC
# MAGIC , ITEM_CHAR_2                                STRING 
# MAGIC
# MAGIC , ITEM_CHAR_3                                STRING 
# MAGIC
# MAGIC , ITEM_CHAR_4                                STRING 
# MAGIC
# MAGIC , ITEM_CHAR_5                                STRING 
# MAGIC
# MAGIC , RESERVED_1                                 STRING 
# MAGIC
# MAGIC , RESERVED_2                                 STRING 
# MAGIC
# MAGIC , RESERVED_3                                 BIGINT 
# MAGIC
# MAGIC , RESERVED_4                                 BIGINT 
# MAGIC
# MAGIC , MISC_1_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , MISC_2_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , MISC_3_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , MISC_4_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , MISC_5_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , MISC_6_BORROWED_FLAG                       TINYINT 
# MAGIC
# MAGIC , MISC_1_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , MISC_2_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , MISC_3_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , MISC_4_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , MISC_5_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , MISC_6_CAN_BORROW_FLAG                     TINYINT 
# MAGIC
# MAGIC , DISCONTINUED_FLAG                          TINYINT 
# MAGIC
# MAGIC , MANUAL_FLAG                                TINYINT 
# MAGIC
# MAGIC , NEW_FLAG                                   TINYINT 
# MAGIC
# MAGIC , PROCESSED_FLAG                             TINYINT 
# MAGIC
# MAGIC , WM_MOD_USER                                STRING 
# MAGIC
# MAGIC , WM_CREATE_TSTMP                            TIMESTAMP 
# MAGIC
# MAGIC , WM_MOD_TSTMP                               TIMESTAMP 
# MAGIC
# MAGIC , DELETE_FLAG                                TINYINT 
# MAGIC
# MAGIC , UPDATE_TSTMP                               TIMESTAMP                   not null
# MAGIC
# MAGIC , LOAD_TSTMP                                 TIMESTAMP                   not null  
# MAGIC
# MAGIC )
# MAGIC USING delta 
# MAGIC LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_item_facility_slotting_dup' 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into refine.WM_ITEM_FACILITY_SLOTTING_dup
# MAGIC select distinct *
# MAGIC from refine.WM_ITEM_FACILITY_SLOTTING
# MAGIC where LOCATION_ID=3
# MAGIC and WM_ITEM_FACILITY_MAPPING_ID=18905

# COMMAND ----------

# MAGIC %sql select * from refine.WM_ITEM_FACILITY_SLOTTING_dup

# COMMAND ----------

# MAGIC %sql delete from refine.WM_ITEM_FACILITY_SLOTTING
# MAGIC where LOCATION_ID=3
# MAGIC and WM_ITEM_FACILITY_MAPPING_ID=18905

# COMMAND ----------

# MAGIC %sql insert into refine.WM_ITEM_FACILITY_SLOTTING
# MAGIC select * from refine.WM_ITEM_FACILITY_SLOTTING_dup

# COMMAND ----------

# MAGIC %sql select LOCATION_ID , WM_ITEM_FACILITY_MAPPING_ID, count(*) 
# MAGIC from refine.WM_ITEM_FACILITY_SLOTTING
# MAGIC group by LOCATION_ID , WM_ITEM_FACILITY_MAPPING_ID
# MAGIC having count(*)>1;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS refine.WM_ITEM_FACILITY_SLOTTING_dup

# COMMAND ----------

dbutils.fs.rm('gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_item_facility_slotting_dup', True)
