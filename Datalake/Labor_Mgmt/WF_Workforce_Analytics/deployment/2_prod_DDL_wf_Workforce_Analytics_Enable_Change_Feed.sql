--enables the cdf feature for the following tables
use legacy;
ALTER TABLE WFA_FCST_SLS_TASK SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE WFA_FCST_SLS_DEPT SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

use empl_protected;
ALTER TABLE legacy_WFA_TSCHD SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE legacy_WFA_TDTL SET TBLPROPERTIES (delta.enableChangeDataFeed = true);