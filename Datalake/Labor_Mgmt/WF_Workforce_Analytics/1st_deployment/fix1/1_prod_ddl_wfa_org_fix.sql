-- Databricks notebook source
DROP TABLE legacy.WFA_ORG;

CREATE TABLE refine.WFA_ORG_history 
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/legacy/wfa_org';

-- COMMAND ----------
use legacy;

CREATE TABLE
    WFA_ORG (
        ORG_ID BIGINT not null,
        ORG_IDS_ID BIGINT,
        ORG_RPT_TO_ID BIGINT,
        ORG_LVL_NBR BIGINT,
        ORG_TYP_ID BIGINT,
        ORG_TYP_NAM STRING,
        ORG_TYP_DES STRING,
        ORG_TYP_DISP_NBR BIGINT,
        ORG_JOB_SWT INT,
        ORG_RT_SWT INT,
        ORG_EFF_DAT TIMESTAMP not null,
        ORG_EXP_DAT TIMESTAMP,
        ORG_LVL06_NAM STRING,
        ORG_LVL07_NAM STRING,
        ORG_LVL08_NAM STRING,
        ORG_LVL09_NAM STRING,
        ORG_LVL10_NAM STRING,
        REC_ACTV_SWT INT,
        REC_EXP_DTM TIMESTAMP,
        LBRLVL_ID BIGINT,
        LBRLVL_DISP_NBR BIGINT,
        LBRLVL_NAM STRING,
        LBRLVL_ENTRY_ID BIGINT,
        LBRLVL_ENTRY_NAM STRING,
        LBRLVL_ENTRY_DES STRING,
        LBRLVL_ENTRY_ACTV_SWT INT,
        UPDATE_DT TIMESTAMP,
        LOAD_DT TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/labor_mgmt/wfa_org' ;
