CREATE TABLE legacy.automated_call_holidays (
  SERVICE_NAME STRING,
  HOLIDAY_NAME STRING,
  HOLIDAY_DATE DATE,
  COUNTRY_CD STRING,
  IS_CLOSED INT,
  LOAD_TSTMP TIMESTAMP)
USING delta
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/IT/automated_call_holidays';


insert into legacy.automated_call_holidays select * from qa_legacy.automated_call_holidays1;

CREATE TABLE legacy.automated_call_rules (
  SERVICE_NAME STRING,
  RULE_NAME STRING,
  PEAK_START_DATE DATE,
  PEAK_END_DATE DATE,
  COUNTRY_CD STRING,
  DAY_OFFSET INT,
  LOAD_TSTMP TIMESTAMP)
USING delta
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/IT/automated_call_rules';

insert into legacy.automated_call_rules select * from qa_legacy.automated_call_rules1;
