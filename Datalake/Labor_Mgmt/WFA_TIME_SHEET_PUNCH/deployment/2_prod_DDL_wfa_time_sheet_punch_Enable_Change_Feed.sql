--enables the cdf feature for the following tables
use legacy;
ALTER TABLE WFA_TIME_SHEET_PUNCH SET TBLPROPERTIES (delta.enableChangeDataFeed = true);