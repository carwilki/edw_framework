INSERT INTO stranger_things.snowflake_cdc_log 
select "empl_protected","legacy_WFA_TDTL","EDW_PRD","public","wfa_tdtl_lgcy",version,CURRENT_TIMESTAMP 
from (SELECT max(version) as version FROM (DESCRIBE HISTORY empl_protected.legacy_WFA_TDTL));

INSERT INTO stranger_things.snowflake_cdc_log 
select "empl_protected","legacy_WFA_TSCHD","EDW_PRD","public","wfa_tschd_lgcy",version,CURRENT_TIMESTAMP 
from (SELECT max(version) as version FROM (DESCRIBE HISTORY empl_protected.legacy_WFA_TSCHD));