CREATE TABLE stranger_things.snowflake_cdc_log (
  dlSchema STRING,
  dlTable STRING,
  targetDatabase STRING,
  targetSchema STRING,
  targetTable STRING,
  version BIGINT,
  timestamp TIMESTAMP)
USING delta
LOCATION 'gs://petm-bdpl-prod-systemdb-p1-gcs-gbl/metadata/tables/snowflake_cdc_log';

 
INSERT INTO legacy.snowflake_cdc_log values("legacy","GL_PLAN_FORECAST_MONTH","EDW_PROD","public","gl_plan_forecast_month_lgcy",NULL,CURRENT_DATE);

update legacy.snowflake_cdc_log set version = (SELECT max(version) FROM (DESCRIBE HISTORY legacy.GL_PLAN_FORECAST_MONTH)) where dlTable = "GL_PLAN_FORECAST_MONTH";