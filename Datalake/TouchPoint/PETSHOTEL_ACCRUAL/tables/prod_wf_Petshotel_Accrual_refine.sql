




--*****  Creating table:  "PETSHOTEL_ACCRUAL" , ***** Creating table: "PETSHOTEL_ACCRUAL"

use prod;
CREATE TABLE  PETSHOTEL_ACCRUAL
(
  DAY_DT        TIMESTAMP                             not null

, ACCRUAL_DT                                        TIMESTAMP                             not null

, LOCATION_ID INT not null

, STORE_NBR INT

, TP_INVOICE_NBR BIGINT not null

, SERVICE_START_DT                                  TIMESTAMP 

, SERVICE_END_DT                                    TIMESTAMP 

, LENGTH_OF_STAY SMALLINT

, TP_EXTENDED_PRICE                                 DECIMAL(38,2) 

, PETCOUNT BIGINT

, ACCRUAL_AMT                                       DECIMAL(38,6) 

, EXCH_RATE_PCNT                                    DECIMAL(15,6) 

, WEEK_DT       TIMESTAMP 

, FISCAL_YR SMALLINT

, FISCAL_MO INT

, FISCAL_WK INT

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/touchpoint/petshotel_accrual' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TP_INVOICE_NBR)


