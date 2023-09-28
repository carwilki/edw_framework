--seeds stranger_thinds.snowflake_cdc_log with the max version of the table for cdc
--replication 
-- Databricks notebook source

INSERT INTO stranger_things.snowflake_cdc_log 
select "legacy","WFA_TIME_SHEET_PUNCH","EDW_PROD","public","wfa_time_sheet_punch_lgcy",version,CURRENT_TIMESTAMP 
from (SELECT max(version) as version FROM (DESCRIBE HISTORY legacy.WFA_TIME_SHEET_PUNCH));
