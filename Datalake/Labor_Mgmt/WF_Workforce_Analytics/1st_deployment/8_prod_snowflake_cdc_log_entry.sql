INSERT INTO stranger_things.snowflake_cdc_log 
select "legacy","WFA_FCST_SLS_TASK","EDW_PRD","public","wfa_fcst_sls_task_lgcy",version,CURRENT_TIMESTAMP 
from (SELECT max(version) as version FROM (DESCRIBE HISTORY legacy.WFA_FCST_SLS_TASK));

INSERT INTO stranger_things.snowflake_cdc_log 
select "legacy","WFA_FCST_SLS_DEPT","EDW_PRD","public","wfa_fcst_sls_dept_lgcy",version,CURRENT_TIMESTAMP 
from (SELECT max(version) as version FROM (DESCRIBE HISTORY legacy.WFA_FCST_SLS_DEPT));

INSERT INTO stranger_things.snowflake_cdc_log 
select "legacy","WFA_TDTL","EDW_PRD","public","wfa_tdtl_lgcy",version,CURRENT_TIMESTAMP 
from (SELECT max(version) as version FROM (DESCRIBE HISTORY empl_protected.legacy_WFA_TDTL));

INSERT INTO stranger_things.snowflake_cdc_log 
select "legacy","WFA_TSCHD","EDW_PRD","public","wfa_tschd_lgcy",version,CURRENT_TIMESTAMP 
from (SELECT max(version) as version FROM (DESCRIBE HISTORY empl_protected.legacy_WFA_TSCHD));