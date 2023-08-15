CREATE TABLE raw.historical_run_details_from_sf (
  job_id STRING,
  run_id STRING,
  task_name STRING,
  process STRING,
  table_name STRING,
  sf_rowCount STRING,
  delta_rowCount STRING,
  status STRING,
  error STRING,
  run_date TIMESTAMP)
USING delta
LOCATION "gs://petm-bdpl-prod-raw-p1-gcs-gbl/historical_run_details_from_sf";


CREATE TABLE raw.sf2delta_pkey_tstcols (
  tableName STRING,
  pKeys STRING,
  tstmp_cols STRING,
  tstmp_cols1 STRING)
USING delta
LOCATION "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sf2delta_pkey_tstcols";

insert into raw.sf2delta_pKey_tstcols select * from dev_raw.sf2delta_pkey_tstcols_external;
