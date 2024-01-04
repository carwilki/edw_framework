--*****  Creating table:  "USER_RANKING_HIERARCHY" , ***** Creating table: "USER_RANKING_HIERARCHY"


use legacy;
 CREATE TABLE USER_RANKING_HIERARCHY 
(
 HIERARCHY_LVL                                     STRING                 not null

, HIERARCHY_SUB_LVL                                 STRING                 not null

, HIERARCHY_NAME                                    STRING                 not null

, RANK_NAME     STRING                 not null

, RANK_CRITERIA                                     STRING                not null

, RANK_VALUE_TYPE                                   STRING                not null

, RANK_MIN_VALUE                                    DECIMAL(20,6) 

, RANK_MAX_VALUE                                    DECIMAL(20,6) 

, UPDATE_DT DATE

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/user_ranking_hierarchy';

--DISTRIBUTE ON (HIERARCHY_LVL, HIERARCHY_SUB_LVL, HIERARCHY_NAME, RANK_NAME)

-- inserting records from qa_legacy to legacy
Insert into legacy.USER_RANKING_HIERARCHY
select * from qa_legacy.USER_RANKING_HIERARCHY;