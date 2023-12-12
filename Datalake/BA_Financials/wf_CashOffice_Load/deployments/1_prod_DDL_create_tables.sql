
--*****  Creating table:  "CO_DEPOSIT" , ***** Creating table: "CO_DEPOSIT"


use legacy;
 CREATE TABLE CO_DEPOSIT 
(  SALES_DT                                  TIMESTAMP                            not null

, SITE_NBR SMALLINT not null

, COUNTRY_CD                                STRING                 not null

, DEPOSIT_TYPE_CD                           STRING                 not null

, GL_ACCT                                   STRING                 not null

, TENDER_TYPE_ID SMALLINT not null

, DEPOSIT_SLIP_NBR BIGINT not null

, DEPOSIT_BAG_NBR BIGINT not null

, SEQ_NBR BIGINT not null

, DEPOSIT_AMT                               DECIMAL(10,2) 

, LOAD_TSTMP                                TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/co_deposit';

--DISTRIBUTE ON (SITE_NBR)


--*****  Creating table:  "CO_HEADER" , ***** Creating table: "CO_HEADER"


use legacy;
 CREATE TABLE CO_HEADER 
(  SALES_DT                                  TIMESTAMP                            not null

, SITE_NBR SMALLINT not null

, SOFTWARE_VERSION                          STRING 

, FOREIGN_EXCHANGE_RT                       DECIMAL(10,4) 

, LOAD_TSTMP                                TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/co_header';

--DISTRIBUTE ON (SITE_NBR)



--*****  Creating table:  "CO_VARIANCE" , ***** Creating table: "CO_VARIANCE"


use legacy;
 CREATE TABLE CO_VARIANCE 
(  SALES_DT                                  TIMESTAMP                            not null

, SITE_NBR SMALLINT not null

, COUNTRY_CD                                STRING                 not null

, VARIANCE_TYPE_CD                          STRING                 not null

, TENDER_TYPE_ID SMALLINT not null

, CASHIER_NBR INT not null

, VARIANCE_AMT                              DECIMAL(10,2) 

, LOAD_TSTMP                                TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/co_variance';

--DISTRIBUTE ON (SITE_NBR)







--*****  Creating table:  "CO_TENDER_TOTAL" , ***** Creating table: "CO_TENDER_TOTAL"


use legacy;
 CREATE TABLE CO_TENDER_TOTAL 
(  SALES_DT                                  TIMESTAMP                            not null

, SITE_NBR SMALLINT not null

, COUNTRY_CD                                STRING                 not null

, TOTAL_TYPE_CD                             STRING                 not null

, TENDER_TYPE_ID SMALLINT not null

, TENDER_TOTAL_AMT                          DECIMAL(10,2) 

, LOAD_TSTMP                                TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/co_tender_total';

--DISTRIBUTE ON (SITE_NBR)




--*****  Creating table:  "CO_DEPOSIT_PRE" , ***** Creating table: "CO_DEPOSIT_PRE"


use raw;
 CREATE TABLE CO_DEPOSIT_PRE 
(  SALES_DT                                  TIMESTAMP                            not null

, SITE_NBR SMALLINT not null

, COUNTRY_CD                                STRING                 not null

, DEPOSIT_TYPE_CD                           STRING                 not null

, GL_ACCT                                   STRING                 not null

, TENDER_TYPE_ID SMALLINT not null

, DEPOSIT_SLIP_NBR BIGINT not null

, DEPOSIT_BAG_NBR BIGINT not null

, SEQ_NBR BIGINT not null

, DEPOSIT_AMT                               DECIMAL(10,2) 

, LOAD_TSTMP                                TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/co_deposit_pre';

--DISTRIBUTE ON (SITE_NBR)







--*****  Creating table:  "CO_HEADER_PRE" , ***** Creating table: "CO_HEADER_PRE"


use raw;
 CREATE TABLE CO_HEADER_PRE 
(  SALES_DT                                  TIMESTAMP                            not null

, SITE_NBR SMALLINT not null

, SOFTWARE_VERSION                          STRING 

, FOREIGN_EXCHANGE_RT                       DECIMAL(10,4) 

, LOAD_TSTMP                                TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/co_header_pre';

--DISTRIBUTE ON (SITE_NBR)







--*****  Creating table:  "CO_VARIANCE_PRE" , ***** Creating table: "CO_VARIANCE_PRE"


use raw;
 CREATE TABLE CO_VARIANCE_PRE 
(  SALES_DT                                  TIMESTAMP                            not null

, SITE_NBR SMALLINT not null

, COUNTRY_CD                                STRING                 not null

, VARIANCE_TYPE_CD                          STRING                 not null

, TENDER_TYPE_ID SMALLINT not null

, CASHIER_NBR INT not null

, VARIANCE_AMT                              DECIMAL(10,2) 

, LOAD_TSTMP                                TIMESTAMP                           

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/co_variance_pre';

--DISTRIBUTE ON (SITE_NBR)







--*****  Creating table:  "CO_TENDER_TOTAL_PRE" , ***** Creating table: "CO_TENDER_TOTAL_PRE"


use raw;
 CREATE TABLE CO_TENDER_TOTAL_PRE 
(  SALES_DT                                  TIMESTAMP                            not null

, SITE_NBR SMALLINT not null

, COUNTRY_CD                                STRING                 not null

, TOTAL_TYPE_CD                             STRING                 not null

, TENDER_TYPE_ID SMALLINT not null

, TENDER_TOTAL_AMT                          DECIMAL(10,2) 

, LOAD_TSTMP                                TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/co_tender_total_pre';

--DISTRIBUTE ON (SITE_NBR)
