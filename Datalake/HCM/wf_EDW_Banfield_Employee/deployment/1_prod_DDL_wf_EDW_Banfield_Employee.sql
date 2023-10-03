
use empl_sensitive;

CREATE TABLE refine_banfield_employee (
  BANF_EMPL_ID BIGINT NOT NULL,
  BANF_EMPL_FIRST_NAME STRING,
  BANF_EMPL_LAST_NAME STRING,
  BANF_EMPL_PETPERKS_ID BIGINT,
  BANF_EMPL_STATUS_CD STRING,
  BANF_EMPL_PIN STRING,
  UPDATE_TSTMP TIMESTAMP,
  LOAD_TSTMP TIMESTAMP)
USING delta
LOCATION 'gs://petm-bdpl-prod-empl-sensitive-refine-p1-gcs-gbl/services/refine_banfield_employee';
