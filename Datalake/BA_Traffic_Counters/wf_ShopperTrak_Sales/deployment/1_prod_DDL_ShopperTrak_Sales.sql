use legacy;

CREATE TABLE ST_FILES_CTRL (
  ST_ID INT NOT NULL,
  ST_DAY_DT DATE NOT NULL,
  ST_CREATE_FILE_FLAG STRING,
  LOAD_TSTMP TIMESTAMP,
  UPDATE_TSTMP TIMESTAMP)
USING delta
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Traffic_Counters/st_files_ctrl'
