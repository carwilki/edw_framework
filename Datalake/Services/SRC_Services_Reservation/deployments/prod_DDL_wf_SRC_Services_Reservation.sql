--*****  Creating table:  "SRC_SERVICES_RESERVATION" , ***** Creating table: "SRC_SERVICES_RESERVATION"


use cust_sensitive;
 CREATE TABLE legacy_SRC_SERVICES_RESERVATION 
( CREATE_DT    DATE                                 not null

, TP_CUSTOMER_NBR BIGINT not null

, TP_INVOICE_NBR BIGINT

, SKU_DESC      STRING 

, SAP_CLASS_DESC                                    STRING 

, LOCATION_ID INT not null

, STORE_NBR INT

, COUNTRY_CD                                        STRING 

, CUST_FIRST_NAME                                   STRING 

, CUST_LAST_NAME                                    STRING 

, CUST_EMAIL_ADDRESS                                STRING 

, SRC_CC_ID     STRING 

, COMMUNICATION_CHANNEL                             STRING 

, EMAIL_OPT_OUT_FLAG INT

, SENT_FLAG INT

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-cust-sensitive-nzlegacy-p1-gcs-gbl/services/src_services_reservation' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TP_CUSTOMER_NBR)

--ORGANIZE   ON (CREATE_DT)


--*****  Creating table:  "SRC_SERVICES_RESERVATION_PRE" , ***** Creating table: "SRC_SERVICES_RESERVATION_PRE"


use cust_sensitive;
 CREATE TABLE raw_SRC_SERVICES_RESERVATION_PRE 
( CREATE_DT    DATE                                 not null

, TP_CUSTOMER_NBR BIGINT not null

, TP_INVOICE_NBR BIGINT

, SKU_DESC      STRING 

, SAP_CLASS_DESC                                    STRING 

, LOCATION_ID INT not null

, STORE_NBR INT

, COUNTRY_CD                                        STRING 

, CUST_FIRST_NAME                                   STRING 

, CUST_LAST_NAME                                    STRING 

, CUST_EMAIL_ADDRESS                                STRING 

, SRC_CC_ID     STRING 

, COMMUNICATION_CHANNEL                             STRING 

, EMAIL_OPT_OUT_FLAG INT

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-cust-sensitive-raw-p1-gcs-gbl/services/src_services_reservation_pre' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TP_CUSTOMER_NBR)

--ORGANIZE   ON (CREATE_DT)


CREATE OR REPLACE VIEW enterprise.CUSTOMER_LOYALTY AS
select 
cla.CUSTOMER_EID AS CUSTOMER_EID
, cla.PERSON_ID AS CUSTOMER_KEY
, cla.LOYALTY_NBR AS LOYALTY_NBR
, CASE WHEN is_member ('PII_Customer') THEN cm.CUSTOMER_FIRST_NM ELSE '*****' END AS FIRST_NAME
, CASE WHEN is_member ('PII_Customer') THEN cm.CUSTOMER_LAST_NM ELSE '*****' END AS LAST_NAME
, CAST(cla.SRC_UPDATE_TS AS DATE) AS START_DT
, CASE WHEN cla.loyalty_Active_ind = 0 THEN CAST(cla.SRC_UPDATE_TS AS DATE) ELSE NULL END AS END_DT
, cla.LOYALTY_ACTIVE_IND AS ACTIVE_FLG
, cla.PROFILE_COMPLETE_IND AS PROFILE_COMPLETE_FLG
, CASE WHEN pl.ONLINE_PROFILE_FLAG='true' THEN 1 
    WHEN pl.ONLINE_PROFILE_FLAG='false' THEN 0 
    ELSE NULL END AS ONLINE_CUSTOMER_FLG
, cla.ENROLLMENT_SITE_ID AS ENROLLMENT_SOURCE
, CAST(cla.ENROLLMENT_TS AS DATE) AS ENROLLMENT_DATE
, pp.NO_PET_FLAG AS N0_PET_FLG
, CASE WHEN pp.NO_PET_FLAG='true' THEN 1 
    WHEN pp.NO_PET_FLAG='false' THEN 0 
    ELSE NULL END AS N0_PET_FLG2
, CASE WHEN a.AIMIA_TXN_TYPE_EXT_REF IS NOT NULL THEN 1 ELSE 0 END AS SIGNUP_REWARD_FLG
, cla.PROFILE_COMPLETE_REWARD_TS AS PROFILE_COMPLETE_REWARD_FLG
, cpm.PET_ACTIVE_IND AS ACTIVE_PET_FLG
, CASE WHEN is_member ('PII_Customer') THEN cem.EMAIL_ADDR 
    ELSE regexp_replace(cem.EMAIL_ADDR,'.+\@','*****@') END AS LOYALTY_EMAIL
, cem.SRC_LOAD_TS AS EMAIL_CREATE_TSTMP
, cem.SRC_UPDATE_TS AS EMAIL_UPDATE_TSTMP
, cla.CURRENT_POINTS_AVAILABLE_CNT AS POINT_BALANCE
, cla.POINTS_1_EXP_CNT AS POINT_1_CNT
, cla.POINTS_1_EXP_DT AS POINT_1_EXP_DT
, cla.POINTS_2_EXP_CNT AS POINT_2_CNT
, cla.POINTS_2_EXP_DT AS POINT_2_EXP_DT
, cla.POINTS_3_EXP_CNT AS POINT_3_CNT
, cla.POINTS_3_EXP_DT AS POINT_3_EXP_DT
, cla.POINTS_4_EXP_CNT AS POINT_4_CNT
, cla.POINTS_4_EXP_DT AS POINT_4_EXP_DT
, cla.POINTS_5_EXP_CNT AS POINT_5_CNT
, cla.POINTS_5_EXP_DT AS POINT_5_EXP_DT
, cla.POINTS_6_EXP_CNT AS POINT_6_CNT
, cla.POINTS_6_EXP_DT AS POINT_6_EXP_DT
, CASE WHEN cem.EMAIL_OPT_STATUS_DESC = 'Opt-Out' THEN 1 ELSE 0 END AS EMAIL_OPT_OUT_FLAG
, cla.LAST_LOYALTY_TXN_DT AS LAST_PROCESS_TSTMP
, cm.MDM_PURGE_TS AS MDM_PURGE_TSTMP
, cla.SRC_LOAD_TS AS SRC_CREATE_TSTMP
, cla.SRC_UPDATE_TS AS SRC_UPDATE_TSTMP
, cla.LOAD_TS AS UPDATE_TSTMP
, cla.UPDATE_TS AS LOAD_TSTMP
from enterprise.customer_loyalty_account cla
join enterprise.customer_email_master cem
  on cla.customer_eid = cem.CUSTOMER_EID
  and cla.PERSON_ID = cem.person_id
join enterprise.customer_master cm
  on cla.CUSTOMER_EID = cm.CUSTOMER_EID
  and cla.PERSON_ID = cm.PERSON_ID  
left join (select distinct customer_eid, PERSON_ID, PET_ACTIVE_IND from enterprise.customer_pet_master where PET_ACTIVE_IND = 1) cpm
  on cla.CUSTOMER_EID = cpm.CUSTOMER_EID
  and cla.PERSON_ID = cpm.PERSON_ID   
join refine.pods_loyalty pl --Get: ONLINE_PROFILE_IND
  on cla.PERSON_ID = pl.person_id
  and cla.LOYALTY_NBR = pl.Loyalty_Nbr
join refine.pods_person pp
  on cla.PERSON_ID = pp.Person_Id --Get: NO_PET_IND
left join refine.aimia_transaction a
  on a.loyalty_nbr = cla.LOYALTY_NBR --Get: SIGNUP_REWARD_FLG
  and a.AIMIA_TXN_TYPE_EXT_REF = 'Enrollment Bonus';

CREATE OR REPLACE enterprise.CUSTOMER_XREF AS
select 
x.CUSTOMER_EID AS CUSTOMER_EID
, x.PETM_SRC_ID AS CUSTOMER_SRC_ID
, x.SRC_SYS_ID AS CUSTOMER_SRC_VALUE
, x.START_DT AS START_DT
, x.END_DT AS END_DT
, CASE WHEN x.PETM_SRC_ID = 100007 THEN x.SRC_SYS_ID ELSE NULL END AS EMPLOYEE_ID
, CASE WHEN is_member ('PII_Employee') AND x.PETM_SRC_ID = 100007 THEN substring(pe.EMPLOYEE_NBR, -4) 
      WHEN pe.EMPLOYEE_NBR is NULL THEN NULL
      ELSE LEFT(substring(pe.EMPLOYEE_NBR, -4), LENGTH(substring(pe.EMPLOYEE_NBR, -4))-4) || '****' 
      END AS EMPLOYEE_EPIN
, CASE WHEN x.PETM_SRC_ID = 100007 THEN cm.EMPLOYEE_IND ELSE NULL END AS EMPLOYEE_ACTIVE_FLG
, x.ACTIVE_IND AS ACTIVE_FLG
, x.UPDATE_TS AS UPDATE_TSTMP
, x.LOAD_TS AS LOAD_TSTMP
from
enterprise.customer_eid_xref x
join enterprise.customer_master cm
  on cm.customer_eid = x.CUSTOMER_EID
left join refine.pods_employee pe
  on cm.person_id = pe.person_id  
where 
  cm.person_id is not null;


