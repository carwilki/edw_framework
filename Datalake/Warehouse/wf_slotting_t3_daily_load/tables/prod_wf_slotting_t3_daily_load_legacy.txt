




--*****  Creating table:  "SLOT_TREE_WEEK" , ***** Creating table: "SLOT_TREE_WEEK"


use legacy;
CREATE TABLE IF NOT EXISTS  SLOT_TREE_WEEK
(
 DC_LOCATION_ID INT not null

, WEEK_DT       TIMESTAMP                             not null

, SL_NODE_ID INT not null

, SL_SLOT_ID INT not null

, SL_SLOT_HT                                        DECIMAL(9,4) 

, SL_SLOT_WIDTH                                     DECIMAL(9,4) 

, SL_SLOT_DEPTH                                     DECIMAL(9,4) 

, SL_WT_LIMIT                                       DECIMAL(13,4) 

, SL_RACK_TYPE INT

, SL_RACK_TYPE_NAME                                 STRING 

, DAY_OF_WK_NBR TINYINT not null

, SL_LVL SMALLINT

, SL_TREE_ORDER INT

, SL_NODE_NAME                                      STRING 

, SL_NODE1      STRING 

, SL_NODE2      STRING 

, SL_NODE3      STRING 

, SL_NODE4      STRING 

, SL_NODE5      STRING 

, SL_NODE6      STRING 

, PRODUCT_ID INT

, SL_SKU_NBR INT

, SL_SKU_DESC                                       STRING 

, SL_PALLET_HI INT

, SL_PALLET_TI INT

, SL_CASE_HT                                        DECIMAL(9,4) 

, SL_CASE_LEN                                       DECIMAL(9,4) 

, SL_CASE_WID                                       DECIMAL(9,4) 

, SL_CASE_WT                                        DECIMAL(9,4) 

, SL_EACH_HT                                        DECIMAL(9,4) 

, SL_EACH_LEN                                       DECIMAL(9,4) 

, SL_EACH_WID                                       DECIMAL(9,4) 

, SL_EACH_WT                                        DECIMAL(9,4) 

, SL_SLOTITEM_ID INT

, SL_CONST_GRP_ID INT

, SL_CONST_GRP_DESC                                 STRING 

, SL_GRP_GRP_ID INT

, SL_SEQ_GRP_ID INT

, SL_SCORE      DECIMAL(9,4) 

, SL_LEGAL_FIT TINYINT

, SL_LEGAL_FIT_REASON INT

, SL_COLOR_ID TINYINT

, SL_COLOR_DESC                                     STRING 

, SL_GROUP_CD                                       STRING 

, SL_GROUP_DESC                                     STRING 

, SL_PKG_TYPE_CD                                    STRING 

, SL_PKG_DESC                                       STRING 

, SL_STATUS_CD                                      STRING 

, SL_STATUS_DESC                                    STRING 

, SL_DSP_SLOT                                       STRING 

, UPDATE_DT DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/warehouse/slot_tree_week' 
 ;

--DISTRIBUTE ON (SL_NODE_ID)







--*****  Creating table:  "SLOT_SLOTTING_GROUPS" , ***** Creating table: "SLOT_SLOTTING_GROUPS"


use legacy;
CREATE TABLE IF NOT EXISTS  SLOT_SLOTTING_GROUPS
(
 SL_GROUP_CD                                       STRING                  not null

, SL_GROUP_DESC                                     STRING                 not null

, UPDATE_DT DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/warehouse/slot_slotting_groups' 
 ;

--DISTRIBUTE ON (SL_GROUP_CD)







--*****  Creating table:  "SLOT_VIOLATION_DETAIL" , ***** Creating table: "SLOT_VIOLATION_DETAIL"


use legacy;
CREATE TABLE IF NOT EXISTS  SLOT_VIOLATION_DETAIL
(
 DC_LOCATION_ID INT not null

, WEEK_DT       TIMESTAMP                             not null

, SL_ITEM_SCORE_ID INT not null

, SL_SLOT_ID INT

, SL_SLOTITEM_ID INT not null

, PRODUCT_ID INT not null

, SL_CATEGORY                                       STRING 

, SL_CAT_NAME                                       STRING 

, SL_CNSTR_GRP_NAME                                 STRING                 not null

, SL_CNSTR_NAME                                     STRING                 not null

, SL_CNSTR_TYPE                                     STRING 

, SL_SCORE      DECIMAL(9,4) 

, SL_IMPORTANCE                                     DECIMAL(9,2) 

, SL_LEGAL_FIT TINYINT

, SL_LEGAL_FIT_REASON INT

, UPDATE_DT DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/warehouse/slot_violation_detail' 
 ;

--DISTRIBUTE ON (SL_SLOT_ID, PRODUCT_ID)







--*****  Creating table:  "SLOT_STATUS_CODES" , ***** Creating table: "SLOT_STATUS_CODES"


use legacy;
CREATE TABLE IF NOT EXISTS  SLOT_STATUS_CODES
(
 SL_STATUS_CD                                      STRING                  not null

, SL_STATUS_DESC                                    STRING                 not null

, UPDATE_DT DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/warehouse/slot_status_codes' 
 ;

--DISTRIBUTE ON (SL_STATUS_CD)







--*****  Creating table:  "SLOT_SCORING_ELEMENTS" , ***** Creating table: "SLOT_SCORING_ELEMENTS"


use legacy;
CREATE TABLE IF NOT EXISTS  SLOT_SCORING_ELEMENTS
(
 SL_ELEMENT_ID INT not null

, SL_COLOR_ID INT not null

, SL_ELEMENT_DESC                                   STRING 

, SL_COLOR_DESC                                     STRING 

, SL_GROUP_MIN                                      DECIMAL(9,2) 

, SL_GROUP_MAX                                      DECIMAL(9,2) 

, UPDATE_DT DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/warehouse/slot_scoring_elements' 
 ;

--DISTRIBUTE ON (SL_ELEMENT_ID, SL_COLOR_ID)







--*****  Creating table:  "SLOT_PKG_TYPES" , ***** Creating table: "SLOT_PKG_TYPES"


use legacy;
CREATE TABLE IF NOT EXISTS  SLOT_PKG_TYPES
(
 SL_PKG_TYPE_CD                                    STRING                  not null

, SL_PKG_TYPE_DESC                                  STRING                 not null

, UPDATE_DT DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/warehouse/slot_pkg_types' 
 ;

--DISTRIBUTE ON (SL_PKG_TYPE_CD)







--*****  Creating table:  "SLOT_PKG_TYPES_PRE" , ***** Creating table: "SLOT_PKG_TYPES_PRE"


use raw;
CREATE TABLE IF NOT EXISTS  SLOT_PKG_TYPES_PRE
(
 SL_PKG_TYPE_CD                                    STRING                  not null

, SL_PKG_TYPE_DESC                                  STRING                 not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/warehouse/slot_pkg_types_pre' 
 ;

--DISTRIBUTE ON (SL_PKG_TYPE_CD)







--*****  Creating table:  "SLOT_VIOLATION_DETAIL_PRE" , ***** Creating table: "SLOT_VIOLATION_DETAIL_PRE"


use raw;
CREATE TABLE IF NOT EXISTS  SLOT_VIOLATION_DETAIL_PRE
(
 LOCATION_ID INT not null

, WEEK_DT       TIMESTAMP                             not null

, SLOT_ITEM_SCORE_ID INT not null

, SLOT_ID INT

, SLOTITEM_ID INT not null

, PRODUCT_ID INT not null

, CATEGORY      STRING 

, CAT_NAME      STRING 

, CNSTR_GRP_NAME                                    STRING                 not null

, CNSTR_NAME                                        STRING                 not null

, CNSTR_TYPE                                        STRING 

, SCORE         DECIMAL(9,4) 

, IMPORTANCE                                        DECIMAL(9,2) 

, LEGAL_FIT TINYINT

, LEGAL_FIT_REASON INT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/warehouse/slot_violation_detail_pre' 
 ;

--DISTRIBUTE ON (SLOT_ID, PRODUCT_ID)







--*****  Creating table:  "SLOT_SCORING_ELEMENTS_PRE" , ***** Creating table: "SLOT_SCORING_ELEMENTS_PRE"


use raw;
CREATE TABLE IF NOT EXISTS  SLOT_SCORING_ELEMENTS_PRE
(
 COLOR_CODE_NBR INT not null

, GROUP_NBR INT not null

, COLOR_CODE_DESC                                   STRING 

, SLOT_COLOR                                        STRING 

, GROUP_MIN     DECIMAL(9,2) 

, GROUP_MAX     DECIMAL(9,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/warehouse/slot_scoring_elements_pre' 
 ;

--DISTRIBUTE ON (COLOR_CODE_NBR, GROUP_NBR)







--*****  Creating table:  "SLOT_STATUS_CODES_PRE" , ***** Creating table: "SLOT_STATUS_CODES_PRE"


use raw;
CREATE TABLE IF NOT EXISTS  SLOT_STATUS_CODES_PRE
(
 SL_STATUS_CD                                      STRING                  not null

, SL_STATUS_DESC                                    STRING                 not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/warehouse/slot_status_codes_pre' 
 ;

--DISTRIBUTE ON (SL_STATUS_CD)







--*****  Creating table:  "SLOT_TREE_WEEK_PRE" , ***** Creating table: "SLOT_TREE_WEEK_PRE"


use raw;
CREATE TABLE IF NOT EXISTS  SLOT_TREE_WEEK_PRE
(
 LOCATION_ID INT not null

, WEEK_DT       TIMESTAMP                             not null

, NODE_ID INT not null

, SLOT_ID INT not null

, SLOT_HT       DECIMAL(9,4) 

, SLOT_WIDTH                                        DECIMAL(9,4) 

, SLOT_DEPTH                                        DECIMAL(9,4) 

, WT_LIMIT      DECIMAL(13,4) 

, RACK_TYPE INT

, RACK_TYPE_NAME                                    STRING 

, DAY_OF_WK_NBR TINYINT not null

, LVL SMALLINT

, TREE_ORDER INT

, TREE          STRING 

, NODE_NAME     STRING 

, NODE1         STRING 

, NODE2         STRING 

, NODE3         STRING 

, NODE4         STRING 

, NODE5         STRING 

, NODE6         STRING 

, PRODUCT_ID INT

, SKU_NBR INT

, SKU_DESC      STRING 

, PALLET_HI INT

, PALLET_TI INT

, CASE_HT       DECIMAL(9,4) 

, CASE_LEN      DECIMAL(9,4) 

, CASE_WID      DECIMAL(9,4) 

, CASE_WT       DECIMAL(9,4) 

, EACH_HT       DECIMAL(9,4) 

, EACH_LEN      DECIMAL(9,4) 

, EACH_WID      DECIMAL(9,4) 

, EACH_WT       DECIMAL(9,4) 

, SLOTITEM_ID INT

, CONST_GRP_ID INT

, SL_CONST_GRP_DESC                                 STRING 

, GRP_GRP_ID INT

, SEQ_GRP_ID INT

, SCORE         DECIMAL(9,4) 

, LEGAL_FIT TINYINT

, LEGAL_FIT_REASON INT

, COLOR_ID TINYINT

, COLOR_DESC                                        STRING 

, GROUP_CD      STRING 

, GROUP_DESC                                        STRING 

, PKG_CD        STRING 

, PKG_DESC      STRING 

, STATUS_CD     STRING 

, STATUS_DESC                                       STRING 

, DSP_SLOT      STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/warehouse/slot_tree_week_pre' 
 ;

--DISTRIBUTE ON (NODE_ID)







--*****  Creating table:  "SLOT_SLOTTING_GROUPS_PRE" , ***** Creating table: "SLOT_SLOTTING_GROUPS_PRE"


use raw;
CREATE TABLE IF NOT EXISTS  SLOT_SLOTTING_GROUPS_PRE
(
 SL_GROUP_CD                                       STRING                  not null

, SL_GROUP_DESC                                     STRING                 not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/warehouse/slot_slotting_groups_pre' 
 ;

--DISTRIBUTE ON (SL_GROUP_CD)


