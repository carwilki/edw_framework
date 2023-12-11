-- Databricks notebook source
--*****  Creating table:  "USR_90DAY_TO_PLAN" , ***** Creating table: "USR_90DAY_TO_PLAN"


use legacy;
 CREATE TABLE USR_90DAY_TO_PLAN 
(
 AVG_EMPL_CNT                                      DECIMAL(18,2) 

, FISCAL_YR INT

, LOCATION_ID INT

, PLAN_TERM_CNT INT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/usr_90day_to_plan';

--DISTRIBUTE ON RANDOM


-- COMMAND ----------



--*****  Creating table:  "USR_PETSAFETY_TO_PLAN" , ***** Creating table: "USR_PETSAFETY_TO_PLAN"


use legacy;
 CREATE TABLE USR_PETSAFETY_TO_PLAN 
(
 FISCAL_YR INT

, LOCATION_ID INT

, PS_PLAN_RATE INT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/usr_petsafety_to_plan';

--DISTRIBUTE ON RANDOM

