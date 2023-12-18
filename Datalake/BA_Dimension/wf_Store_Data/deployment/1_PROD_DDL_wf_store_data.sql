




--*****  Creating table:  "STORE_DATA" , ***** Creating table: "STORE_DATA"


use legacy;
 CREATE TABLE IF EXISTS  STORE_DATA 
(
 SITE_NBR SMALLINT not null

, COMPANY_CD SMALLINT

, CURRENCY_CD                                       STRING 

, MERCH_ID      STRING 

, TAX_JURISDICTION_CD                               STRING 

, EARLIEST_VALUE_POST_DT                            TIMESTAMP 

, TOT_VALUE_WARNING_LMT                             DECIMAL(10,2) 

, TOT_VALUE_ERROR_LMT                               DECIMAL(10,2) 

, TRANS_CNT_LOWER_LMT INT

, TRANS_CNT_WARNING_LMT INT

, TRANS_CNT_ERR_LMT INT

, TRANS_VALUE_ERR_LMT                               DOUBLE 

, TRANS_SEQ_GAP_CNT_ERR_LMT INT

, OUT_OF_BAL_CNT                                    DECIMAL(10,2) 

, OUT_OF_BAL_VALUE                                  DECIMAL(10,2) 

, BANK_DEPOSIT_VAR_PCT                              DECIMAL(10,2) 

, CREATE_DT     TIMESTAMP 

, CREATE_USER                                       STRING 

, UPDATE_DT     TIMESTAMP 

, UPDATE_USER                                       STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/store_data';

--DISTRIBUTE ON RANDOM


