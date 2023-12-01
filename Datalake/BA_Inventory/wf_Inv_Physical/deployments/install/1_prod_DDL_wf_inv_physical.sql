
--*****  Creating table:  "INV_PHYS_OVERSTOCK_LOC_TYPE" , ***** Creating table: "INV_PHYS_OVERSTOCK_LOC_TYPE"


use legacy;
CREATE TABLE INV_PHYS_OVERSTOCK_LOC_TYPE
(
 OVERSTOCK_LOC_TYPE_EFF_DT                        DATE                                not null

, OVERSTOCK_LOC_TYPE_ID SMALLINT not null

, OVERSTOCK_LOC_TYPE_END_DT DATE

, OVERSTOCK_LOC_TYPE_DESC                           STRING 

, LIMIT_LOW_END INT

, LIMIT_HIGH_END INT

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/inv_phys_overstock_loc_type' ;

--DISTRIBUTE ON (OVERSTOCK_LOC_TYPE_EFF_DT, OVERSTOCK_LOC_TYPE_ID)



--*****  Creating table:  "INV_PHYSICAL" , ***** Creating table: "INV_PHYSICAL"


use legacy;
CREATE TABLE INV_PHYSICAL
(
 POSTING_DT                                       DATE                                not null

, DOC_NBR       STRING                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, CLIENT        STRING 

, FISCAL_YR INT

, SITE_NBR      STRING 

, SKU_NBR INT

, BOOK_QTY      DECIMAL(13,3) 

, PHYSICAL_QTY                                      DECIMAL(13,3) 

, DIFF_QTY      DECIMAL(13,3) 

, OVERSTOCK_QTY                                     DECIMAL(13,3) 

, DIFF_AMT      DECIMAL(13,2) 

, TRANS_TYPE                                        STRING 

, STORAGE_LOC                                       STRING 

, SPECIAL_STOCK_IND                                 STRING 

, DOC_DT DATE

, PLANNED_DT DATE

, LAST_COUNT_DT DATE

, FISCAL_PERIOD INT

, CHANGED_BY                                        STRING 

, POSTING_BLOCK                                     STRING 

, COUNT_STATUS                                      STRING 

, ADJ_POSTING_STATUS                                STRING 

, PHYS_INV_REF_NBR                                  STRING 

, DELETE_FLAG_STATUS                                STRING 

, BOOK_INV_FREEZE                                   STRING 

, GROUP_CRIT_TYPE                                   STRING 

, GROUP_CRIT_PI                                     STRING 

, PHYS_INV_TYPE_DESC                                STRING 

, PHYS_INV_DOC_DESC                                 STRING 

, PHYS_INV_TYPE_ID                                  STRING 

, INV_BALANCE_STATUS                                STRING 

, LN_NBR INT

, BATCH_NBR     STRING 

, PHYS_INV_STOCK_TYPE_ID                            STRING 

, SALES_ORDER_NBR                                   STRING 

, ITEM_NBR INT

, DELIVERY_SCHED INT

, VENDOR_NBR                                        STRING 

, ACCT_CUST_NBR                                     STRING 

, DISTRIBUTION_DIFF                                 STRING 

, CHANGED_DT DATE

, COUNTED_BY                                        STRING 

, ADJ_POSTING_BY                                    STRING 

, ITEM_COUNTED_FLAG                                 STRING 

, DIFFERENCE_POSTED                                 STRING 

, ITEM_RECOUNTED_FLAG                               STRING 

, ITEM_DELETED_FLAG                                 STRING 

, ALT_UOM_FLAG                                      STRING 

, ZERO_COUNT_FLAG                                   STRING 

, BASE_UOM      STRING 

, QTY_UOM       DECIMAL(13,3) 

, PHYS_INV_UOM                                      STRING 

, NBR_ARTICLE_DOC                                   STRING 

, ARTICLE_DOC_YR INT

, ITEM_ARTICLE_DOC INT

, NBR_RECOUNT_DOC                                   STRING 

, CURRENCY_CD                                       STRING 

, EXCH_RATE_PCT                                     DECIMAL(9,6) 

, PHYS_INV_CYCLE_COUNT_IND                          STRING 

, WBS_ELEMENT INT

, SALES_PRICE_TAX_INC                               DECIMAL(13,2) 

, EXT_SALES_VALUE_LOCAL                             DECIMAL(13,2) 

, BOOK_VALUE_SP                                     DECIMAL(13,2) 

, INV_FLAG      STRING 

, SALES_PRICE_TAX_EXC                               DECIMAL(13,2) 

, SALES_INV_DIFF_VAT                                DECIMAL(13,2) 

, SALES_INV_DIFF_NOVAT                              DECIMAL(13,2) 

, PHYS_INV_COUNT_VALUE                              DECIMAL(13,2) 

, BOOK_QTY_VALUE                                    DECIMAL(13,2) 

, INV_DIFF      DECIMAL(13,2) 

, ARTCILE_CATEGORY                                  STRING 

, INV_DIFF_REASON                                   SMALLINT 

, XSITE_CONF_ARTICLE                                STRING 

, PHYS_INV_DIST_DIFF                                STRING 

, DATE_COUNT_OT DATE

, TIME_COUNT_OT DATE

, FREEZE_DT_INV_BALANCE DATE

, FREEZE_TIME_INV_BALANCE DATE

, BOOK_QTY_CHANGED                                  DECIMAL(13,3) 

, RETAIL_VALUE                                      DECIMAL(13,2) 

, BOOK_INV      STRING 

, PHYS_INV_ENTRY_DATE DATE

, PHYS_INV_ENTRY_TIME DATE

, PHYS_INV_DIFF_ADJ_TYPE_ID INT

, UPDATE_DT DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/inv_physical';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "INV_PHYS_OVERSTOCK_LOC" , ***** Creating table: "INV_PHYS_OVERSTOCK_LOC"


use legacy;
CREATE TABLE INV_PHYS_OVERSTOCK_LOC
(
 POSTING_DT                                       DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, OVERSTOCK_LOC_NBR INT not null

, OVERSTOCK_LOC_TYPE_ID SMALLINT

, PLANNED_DT DATE

, COUNT_QTY     INT 

, POG_LISTED_IND SMALLINT

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/inv_phys_overstock_loc' ;

--DISTRIBUTE ON (POSTING_DT, LOCATION_ID, PRODUCT_ID, OVERSTOCK_LOC_NBR)







--*****  Creating table:  "SAP_IKPF_PRE" , ***** Creating table: "SAP_IKPF_PRE"


use raw;
CREATE TABLE SAP_IKPF_PRE
(
 MANDT         STRING                  not null

, DOC_NBR       STRING                 not null

, FISCAL_YR INT not null

, VGART         STRING 

, SITE_NBR      STRING 

, LGORT         STRING 

, SOBKZ         STRING 

, BLDAT DATE

, GIDAT DATE

, ZLDAT DATE

, POSTING_DT DATE

, MONAT INT

, USNAM         STRING 

, SPERR         STRING 

, ZSTAT         STRING 

, DSTAT         STRING 

, XBLNI         STRING 

, LSTAT         STRING 

, XBUFI         STRING 

, KEORD         STRING 

, ORDNG         STRING 

, INVNU         STRING 

, IBLTXT        STRING 

, INVART        STRING 

, WSTI_BSTAT                                        STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/sap_ikpf_pre' ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "SAP_ISEG_PRE" , ***** Creating table: "SAP_ISEG_PRE"


use raw;
CREATE TABLE SAP_ISEG_PRE
(
 MANDT         STRING                  not null

, DOC_NBR       STRING                 not null

, FISCAL_YR INT not null

, ZEILI INT not null

, SKU_NBR       STRING 

, SITE_NBR      STRING 

, LGORT         STRING 

, CHARG         STRING 

, SOBKZ         STRING 

, BSTAR         STRING 

, KDAUF         STRING 

, KDPOS INT

, KDEIN INT

, LIFNR         STRING 

, KUNNR         STRING 

, PLPLA         STRING 

, USNAM         STRING 

, AEDAT DATE

, USNAZ         STRING 

, ZLDAT DATE

, USNAD         STRING 

, BUDAT DATE

, XBLNI         STRING 

, XZAEL         STRING 

, XDIFF         STRING 

, XNZAE         STRING 

, XLOEK         STRING 

, XAMEI         STRING 

, BOOK_QTY      DECIMAL(13,3) 

, XNULL         STRING 

, PHYSICAL_QTY                                      DECIMAL(13,3) 

, MEINS         STRING 

, ERFMG         DECIMAL(13,3) 

, ERFME         STRING 

, MBLNR         STRING 

, MJAHR INT

, ZEILE INT

, NBLNR         STRING 

, DIFF_AMT      DECIMAL(13,2) 

, WAERS         STRING 

, ABCIN         STRING 

, PS_PSP_PNR INT

, VKWRT         DECIMAL(13,2) 

, EXVKW         DECIMAL(13,2) 

, BUCHW         DECIMAL(13,2) 

, KWART         STRING 

, VKWRA         DECIMAL(13,2) 

, VKMZL         DECIMAL(13,2) 

, VKNZL         DECIMAL(13,2) 

, WRTZL         DECIMAL(13,2) 

, WRTBM         DECIMAL(13,2) 

, DIWZL         DECIMAL(13,2) 

, ATTYP         STRING 

, GRUND         SMALLINT 

, SAMAT         STRING 

, XDISPATCH     STRING 

, WSTI_COUNTDATE DATE

, WSTI_COUNTTIME DATE

, WSTI_FREEZEDATE DATE

, WSTI_FREEZETIME DATE

, WSTI_POSM     DECIMAL(13,3) 

, WSTI_POSW     DECIMAL(13,2) 

, WSTI_XCALC                                        STRING 

, WSTI_ENTERDATE DATE

, WSTI_ENTERTIME DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/sap_iseg_pre' ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "ZTB_RF_PHYINV_PRE" , ***** Creating table: "ZTB_RF_PHYINV_PRE"


use raw;
CREATE TABLE ZTB_RF_PHYINV_PRE
(
 MANDT         STRING                  not null

, SITE          STRING                  not null

, LOCATION INT not null

, UPC           STRING                 not null

, ARTICLE       STRING 

, DESCRIPTION                                       STRING 

, COUNT_QTY     INT 

, STATUS        STRING 

, USER_NAME     STRING 

, CREATE_DATE                                       TIMESTAMP 

, POST_DATE     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/ztb_rf_phyinv_pre'; 

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "INV_PHYSICAL_PRE" , ***** Creating table: "INV_PHYSICAL_PRE"


use raw;
CREATE TABLE INV_PHYSICAL_PRE
(
 POSTING_DT                                       DATE                                not null

, DOC_NBR       STRING                not null

, SITE_NBR      STRING                 not null

, SKU_NBR INT not null

, CLIENT        STRING 

, FISCAL_YR INT

, BOOK_QTY      DECIMAL(13,3) 

, PHYSICAL_QTY                                      DECIMAL(13,3) 

, OVERSTOCK_QTY                                     DECIMAL(13,3) 

, DIFF_AMT      DECIMAL(13,2) 

, TRANS_TYPE                                        STRING 

, STORAGE_LOC                                       STRING 

, SPECIAL_STOCK_IND                                 STRING 

, DOC_DT DATE

, PLANNED_DT DATE

, LAST_COUNT_DT DATE

, FISCAL_PERIOD INT

, CHANGED_BY                                        STRING 

, POSTING_BLOCK                                     STRING 

, COUNT_STATUS                                      STRING 

, ADJ_POSTING_STATUS                                STRING 

, PHYS_INV_REF_NBR                                  STRING 

, DELETE_FLAG_STATUS                                STRING 

, BOOK_INV_FREEZE                                   STRING 

, GROUP_CRIT_TYPE                                   STRING 

, GROUP_CRIT_PI                                     STRING 

, PHYS_INV_TYPE_DESC                                STRING 

, PHYS_INV_DOC_DESC                                 STRING 

, INV_BALANCE_STATUS                                STRING 

, LN_NBR INT

, BATCH_NBR     STRING 

, PHYS_INV_STOCK_TYPE_ID                            STRING 

, SALES_ORDER_NBR                                   STRING 

, ITEM_NBR INT

, DELIVERY_SCHED INT

, VENDOR_NBR                                        STRING 

, ACCT_CUST_NBR                                     STRING 

, DISTRIBUTION_DIFF                                 STRING 

, CHANGED_DT DATE

, COUNTED_BY                                        STRING 

, ADJ_POSTING_BY                                    STRING 

, ITEM_COUNTED_FLAG                                 STRING 

, DIFFERENCE_POSTED                                 STRING 

, ITEM_RECOUNTED_FLAG                               STRING 

, ITEM_DELETED_FLAG                                 STRING 

, ALT_UOM_FLAG                                      STRING 

, ZERO_COUNT_FLAG                                   STRING 

, BASE_UOM      STRING 

, QTY_UOM       DECIMAL(13,3) 

, PHYS_INV_UOM                                      STRING 

, NBR_ARTICLE_DOC                                   STRING 

, ARTICLE_DOC_YR INT

, ITEM_ARTICLE_DOC INT

, NBR_RECOUNT_DOC                                   STRING 

, CURRENCY_CD                                       STRING 

, PHYS_INV_CYCLE_COUNT_IND                          STRING 

, WBS_ELEMENT INT

, SALES_PRICE_TAX_INC                               DECIMAL(13,2) 

, EXT_SALES_VALUE_LOCAL                             DECIMAL(13,2) 

, BOOK_VALUE_SP                                     DECIMAL(13,2) 

, INV_FLAG      STRING 

, SALES_PRICE_TAX_EXC                               DECIMAL(13,2) 

, SALES_INV_DIFF_VAT                                DECIMAL(13,2) 

, SALES_INV_DIFF_NOVAT                              DECIMAL(13,2) 

, PHYS_INV_COUNT_VALUE                              DECIMAL(13,2) 

, BOOK_QTY_VALUE                                    DECIMAL(13,2) 

, INV_DIFF      DECIMAL(13,2) 

, ARTCILE_CATEGORY                                  STRING 

, INV_DIFF_REASON                                   SMALLINT 

, XSITE_CONF_ARTICLE                                STRING 

, PHYS_INV_DIST_DIFF                                STRING 

, DATE_COUNT_OT DATE

, TIME_COUNT_OT DATE

, FREEZE_DT_INV_BALANCE DATE

, FREEZE_TIME_INV_BALANCE DATE

, BOOK_QTY_CHANGED                                  DECIMAL(13,3) 

, RETAIL_VALUE                                      DECIMAL(13,2) 

, BOOK_INV      STRING 

, PHYS_INV_ENTRY_DATE DATE

, PHYS_INV_ENTRY_TIME DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/inv_physical_pre' ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "INV_PHYS_OVERSTOCK_LOC_PRE" , ***** Creating table: "INV_PHYS_OVERSTOCK_LOC_PRE"


use raw;
CREATE TABLE INV_PHYS_OVERSTOCK_LOC_PRE
(
 POSTING_DT                                       DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, OVERSTOCK_LOC_NBR INT not null

, OVERSTOCK_LOC_TYPE_ID SMALLINT

, PLANNED_DT DATE

, COUNT_QTY     INT 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/inv_phys_overstock_loc_pre';

--DISTRIBUTE ON (POSTING_DT, LOCATION_ID, PRODUCT_ID, OVERSTOCK_LOC_NBR)
