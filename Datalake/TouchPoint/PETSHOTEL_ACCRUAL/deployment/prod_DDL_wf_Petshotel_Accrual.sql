--*****  Creating table:  "PETSHOTEL_ACCRUAL" , ***** Creating table: "PETSHOTEL_ACCRUAL"


use refine;
CREATE TABLE  PETSHOTEL_ACCRUAL
( DAY_DT        TIMESTAMP                             not null

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
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/touchpoint/petshotel_accrual' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TP_INVOICE_NBR)


--*****  Creating table:  "PETSHOTEL_EXCH_RATE_PRE" , ***** Creating table: "PETSHOTEL_EXCH_RATE_PRE"


use raw;
CREATE TABLE  PETSHOTEL_EXCH_RATE_PRE
( DAY_DT        TIMESTAMP                             not null

, COUNTRY_CD                                        STRING                          not null

, EXCH_RATE_PCT                                     DECIMAL(9,6) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/touchpoint/petshotel_exch_rate_pre' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (DAY_DT)

use enterprise;
CREATE OR REPLACE VIEW  CURRENCY_DAY AS
SELECT DISTINCT DAY_DT
  ,CURRENCY_TYPE_FROM_CD CURRENCY_ID
  ,DATE_FORMAT(CURRENCY_EXCH_RATE_EFF_DT, 'yyyy-MM-dd HH:mm:ss') DATE_RATE_START
  ,'Canadian Dollar' AS CURRENCY_TYPE
  ,DATE_FORMAT(CURRENCY_EXCH_RATE_END_DT, 'yyyy-MM-dd HH:mm:ss') DATE_RATE_ENDED
  ,CURRENCY_EXCH_RT EXCHANGE_RATE_PCNT
  ,1 RATIO_TO
  ,1 RATIO_FROM
  ,'CA' STORE_CTRY_ABBR
  ,1 CURRENCY_NBR
FROM ENTERPRISE.CURRENCY_EXCH_RATE_DAY
WHERE CURRENCY_TYPE_FROM_CD = 'CAD'
ORDER BY DAY_DT DESC;

