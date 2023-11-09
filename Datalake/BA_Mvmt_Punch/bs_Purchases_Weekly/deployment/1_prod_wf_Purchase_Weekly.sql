




--*****  Creating table:  "PURCHASES" , ***** Creating table: "PURCHASES"


use legacy;
 CREATE TABLE  PURCHASES 
(  DAY_DT        TIMESTAMP                             not null

, PRODUCT_ID INT not null

, VENDOR_ID BIGINT not null

, PO_NBR BIGINT not null

, PO_LINE_NBR INT not null

, LOCATION_ID INT not null

, PO_TYPE_ID                                        STRING 

, ORIG_DATE_DUE                                     TIMESTAMP 

, DATE_DUE      TIMESTAMP 

, DELETE_FLAG                                       STRING 

, DELIVERY_FLAG                                     STRING 

, ORDER_COST_ACTUAL                                 DECIMAL(11,3) 

, ORDER_COST_GROSS                                  DECIMAL(11,3) 

, ORDER_COST_NET                                    DECIMAL(11,3) 

, ORIG_ORDER_QTY                                    INT 

, ORDER_QTY     INT 

, SCHED_QTY     INT 

, RECEIPT_FLAG                                      STRING 

, DATE_RECEIVED                                     TIMESTAMP 

, RECEIPTS_QTY                                      INT 

, FREIGHT_COST                                      DECIMAL(9,2) 

, UPDT_CNT TINYINT

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/purchases';

--DISTRIBUTE ON (PO_NBR)



--*****  Creating table:  "PURCH_PERF" , ***** Creating table: "PURCH_PERF"


use legacy;
 CREATE TABLE  PURCH_PERF 
(  WEEK_DT       TIMESTAMP                             not null

, SAP_CATEGORY_ID INT not null

, VENDOR_ID BIGINT not null

, PO_TYPE_ID                                        STRING                  not null

, DISTRICT_ID BIGINT not null

, STORE_TYPE_ID                                     STRING                          not null

, RECVD_AFTER_DUE_DAYS_CNT INT

, RECVD_BEFORE_DUE_DAYS_CNT INT

, ACTUAL_LEADTIME_DAYS_CNT INT

, PO_DUE_CNT INT

, PO_RECVD_CNT INT

, PO_LATE_CNT INT

, LINE_ITEM_DUE_CNT INT

, LINE_ITEM_RECVD_CNT INT

, LINE_ITEMS_LATE_CNT INT

, LINE_ITEM_OOB_CNT INT

, DUE_QTY       INT 

, RECVD_QTY     INT 

, LATE_QTY      INT 

, EXCESS_QTY                                        INT 

, SHORT_QTY     INT 

, ORDER_COST_GROSS                                  DECIMAL(12,2) 

, ORDER_COST_ACTUAL                                 DECIMAL(12,2) 

, ORDER_COST_NET                                    DECIMAL(12,2) 

, FREIGHT_COST                                      DECIMAL(12,2) 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/purch_perf';

--DISTRIBUTE ON (SAP_CATEGORY_ID, DISTRICT_ID)







--*****  Creating table:  "PURCH_PO_NBR_PRE" , ***** Creating table: "PURCH_PO_NBR_PRE"


use raw;
 CREATE TABLE  PURCH_PO_NBR_PRE 
(  WEEK_DT       TIMESTAMP                             not null

, SAP_CATEGORY_ID INT not null

, VENDOR_ID BIGINT not null

, PO_TYPE_ID                                        STRING                  not null

, PO_NBR BIGINT not null

, DISTRICT_ID BIGINT not null

, STORE_TYPE_ID                                     STRING                          not null

, PO_DUE_CNT INT 

, PO_RECVD_CNT INT 

, PO_LATE_CNT INT 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Mvmt_Purch/purch_po_nbr_pre';

--DISTRIBUTE ON (PO_NBR)







--*****  Creating table:  "PURCH_PO_TYPE_PRE" , ***** Creating table: "PURCH_PO_TYPE_PRE"


use raw;
 CREATE TABLE  PURCH_PO_TYPE_PRE 
(  WEEK_DT       TIMESTAMP                             not null

, SAP_CATEGORY_ID INT not null

, VENDOR_ID BIGINT not null

, PO_TYPE_ID                                        STRING                  not null

, DISTRICT_ID BIGINT not null

, STORE_TYPE_ID                                     STRING                          not null

, RECVD_AFTER_DUE_DAYS_CNT INT

, RECVD_BEFORE_DUE_DAYS_CNT INT

, ACTUAL_LEADTIME_DAYS_CNT INT

, LINE_ITEM_DUE_CNT INT

, LINE_ITEM_RECVD_CNT INT

, LINE_ITEMS_LATE_CNT INT

, LINE_ITEM_OOB_CNT INT

, DUE_QTY       INT 

, RECVD_QTY     INT 

, LATE_QTY      INT 

, EXCESS_QTY                                        INT 

, SHORT_QTY     INT 

, ORDER_COST_GROSS                                  DECIMAL(12,2) 

, ORDER_COST_ACTUAL                                 DECIMAL(12,2) 

, ORDER_COST_NET                                    DECIMAL(12,2) 

, FREIGHT_COST                                      DECIMAL(12,2) 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Mvmt_Purch/purch_po_type_pre';

--DISTRIBUTE ON (SAP_CATEGORY_ID, DISTRICT_ID)