--*****  Creating table:  "UDH_PRICING_ROLE_LKUP" , ***** Creating table: "UDH_PRICING_ROLE_LKUP"


use legacy;
CREATE TABLE   UDH_PRICING_ROLE_LKUP
(
PRICING_CATEGORY_ROLE_ID INT not null

, PRICING_CATEGORY_ROLE_DESC                        STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/INT_Pricing/udh_pricing_role_lkup';
--DISTRIBUTE ON (PRICING_CATEGORY_ROLE_ID)



--*****  Creating table:  "UDH_ONLINE_ROLE_LKUP" , ***** Creating table: "UDH_ONLINE_ROLE_LKUP"


use legacy;
CREATE TABLE   UDH_ONLINE_ROLE_LKUP
(
 ONLINE_CATEGORY_ROLE_ID INT not null

, ONLINE_CATEGORY_ROLE_DESC                         STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/INT_Pricing/udh_online_role_lkup';
--DISTRIBUTE ON (ONLINE_CATEGORY_ROLE_ID)




--*****  Creating table:  "SAP_PRICING_DIVISION" , ***** Creating table: "SAP_PRICING_DIVISION"


use legacy;
CREATE TABLE   SAP_PRICING_DIVISION
(
SAP_PRICING_DIVISION_ID INT not null COMMENT "Primary Key"

, SAP_PRICING_DIVISION_DESC                         STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/INT_Pricing/sap_pricing_division';
--DISTRIBUTE ON (SAP_PRICING_DIVISION_ID)




--*****  Creating table:  "SAP_PRICING_CATEGORY" , ***** Creating table: "SAP_PRICING_CATEGORY"


use legacy;
CREATE TABLE   SAP_PRICING_CATEGORY
(
 SAP_PRICING_CATEGORY_ID INT not null COMMENT "Primary Key"

, SAP_PRICING_CLASS_ID INT not null

, SAP_PRICING_CATEGORY_DESC                         STRING 

, PRICING_CATEGORY_ROLE_ID INT

, PRICING_CATEGORY_ROLE_DESC                        STRING 

, ONLINE_CATEGORY_ROLE_ID INT

, ONLINE_CATEGORY_ROLE_DESC                         STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/INT_Pricing/sap_pricing_category';
--DISTRIBUTE ON (SAP_PRICING_CATEGORY_ID)







--*****  Creating table:  "SAP_PRICING_CLASS" , ***** Creating table: "SAP_PRICING_CLASS"


use legacy;
CREATE TABLE   SAP_PRICING_CLASS
(
 SAP_PRICING_CLASS_ID INT not null COMMENT "Primary Key"

, SAP_PRICING_DEPT_ID INT not null

, SAP_PRICING_CLASS_DESC                            STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/INT_Pricing/sap_pricing_class';
--DISTRIBUTE ON (SAP_PRICING_CLASS_ID)







--*****  Creating table:  "SAP_PRICING_DEPT" , ***** Creating table: "SAP_PRICING_DEPT"


use legacy;
CREATE TABLE   SAP_PRICING_DEPT
(
  SAP_PRICING_DEPT_ID INT not null COMMENT "Primary Key"

, SAP_PRICING_DIVISION_ID INT not null

, SAP_PRICING_DEPT_DESC                             STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/INT_Pricing/sap_pricing_dept';
--DISTRIBUTE ON (SAP_PRICING_DEPT_ID)







--*****  Creating table:  "SAP_PRICING_HIERARCHY_ERRORS" , ***** Creating table: "SAP_PRICING_HIERARCHY_ERRORS"


use legacy;
CREATE TABLE   SAP_PRICING_HIERARCHY_ERRORS
(
 MISSING_NODE INT not null COMMENT "Primary Key"

, LOAD_TABLE                                        STRING                 not null COMMENT "Primary Key"

, ERROR_MESSAGE                                     STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/INT_Pricing/sap_pricing_hierarchy_errors';
--DISTRIBUTE ON (MISSING_NODE, LOAD_TABLE)







--*****  Creating table:  "SAP_PRICING_HIERARCHY_PRE" , ***** Creating table: "SAP_PRICING_HIERARCHY_PRE"


use raw;
CREATE TABLE   SAP_PRICING_HIERARCHY_PRE
(
 HIER_NODE     STRING                 not null COMMENT "Primary Key"

, START_DT     DATE                                 not null COMMENT "Primary Key"

, END_DT       DATE                                 not null

, HIER_LEVEL TINYINT not null

, PARENT_NODE                                       STRING                 not null

, CATFLG        STRING 

, ROLE          STRING 

, STRATEGY      STRING 

, PRODUCTCLF                                        STRING 

, PRICE_GROUP                                       STRING 

, REFERENCE_NODE                                    STRING 

, RESPONSABILITY                                    STRING 

, NODE_DESC     STRING 

, UPPER_DESC                                        STRING 

, LONG_DESC     STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/INT_Pricing/sap_pricing_hierarchy_pre';
--DISTRIBUTE ON RANDOM

