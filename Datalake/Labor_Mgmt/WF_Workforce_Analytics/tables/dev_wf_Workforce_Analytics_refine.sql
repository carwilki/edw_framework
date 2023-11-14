--*****  Creating table:  "WFA_TSCHD_PRE" , ***** Creating table: "WFA_TSCHD_PRE"
use dev_empl_protected;

CREATE TABLE
    raw_WFA_TSCHD_PRE (
        TSCHD_ID BIGINT not null,
        TSCHD_DAT TIMESTAMP not null,
        EMP_SKEY BIGINT,
        STRT_DTM TIMESTAMP,
        END_DTM TIMESTAMP,
        PAYCD_SKEY BIGINT,
        CORE_HRS_SWT INT,
        PRI_ORG_SKEY BIGINT,
        ORG_SKEY BIGINT,
        SHIFT_ID BIGINT,
        MONEY_AMT DECIMAL(16, 6),
        DRTN_AMT DECIMAL(16, 6),
        CORE_AMT DECIMAL(16, 6),
        NON_CORE_AMT DECIMAL(16, 6),
        DRTN_HRS DECIMAL(16, 6),
        CORE_HRS DECIMAL(16, 6),
        NON_CORE_HRS DECIMAL(16, 6),
        UPDT_DTM TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-raw-p1-gcs-gbl/labor_mgmt/wfa_tschd_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TSCHD_ID)
--*****  Creating table:  "WFA_TSCHD_PREV_PRE" , ***** Creating table: "WFA_TSCHD_PREV_PRE"
use dev_empl_protected;

CREATE TABLE
    raw_WFA_TSCHD_PREV_PRE (
        TSCHD_ID BIGINT not null,
        TSCHD_DAT TIMESTAMP not null,
        EMP_SKEY BIGINT,
        STRT_DTM TIMESTAMP,
        END_DTM TIMESTAMP,
        PAYCD_SKEY BIGINT,
        CORE_HRS_SWT INT,
        PRI_ORG_SKEY BIGINT,
        ORG_SKEY BIGINT,
        SHIFT_ID BIGINT,
        MONEY_AMT DECIMAL(16, 6),
        DRTN_AMT DECIMAL(16, 6),
        CORE_AMT DECIMAL(16, 6),
        NON_CORE_AMT DECIMAL(16, 6),
        DRTN_HRS DECIMAL(16, 6),
        CORE_HRS DECIMAL(16, 6),
        NON_CORE_HRS DECIMAL(16, 6)
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-raw-p1-gcs-gbl/labor_mgmt/wfa_tschd_prev_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TSCHD_ID)
--*****  Creating table:  "WFA_TDTL_PRE" , ***** Creating table: "WFA_TDTL_PRE"
use dev_empl_protected;

CREATE TABLE
    raw_WFA_TDTL_PRE (
        TDTL_SKEY DECIMAL(20, 0) not null,
        TDTL_ID BIGINT,
        TSITEM_SKEY DECIMAL(20, 0),
        RECORDED_DAT TIMESTAMP,
        RECORDED_FOR_DAT TIMESTAMP,
        ADJ_SWT INT,
        EDT_SWT INT,
        EMP_SKEY BIGINT,
        STRT_DTM TIMESTAMP,
        END_DTM TIMESTAMP,
        UNSCHD_STRT_DTM TIMESTAMP,
        UNSCHD_END_DTM TIMESTAMP,
        STRT_TZONE_SKEY BIGINT,
        LBRACCT_SKEY BIGINT,
        HM_LBRACCT_SWT INT,
        HM_LBRACCT_SKEY BIGINT,
        FROM_LBRACCT_SKEY BIGINT,
        PAYCD_SKEY BIGINT,
        CORE_HRS_SWT INT,
        FROM_PAYCD_SKEY BIGINT,
        SUPV_SKEY BIGINT,
        PRI_JOB_SKEY BIGINT,
        JOB_SKEY BIGINT,
        PRI_ORG_SKEY BIGINT,
        ORG_SKEY BIGINT,
        PAYPER_SKEY BIGINT,
        HDAY_SKEY BIGINT,
        STRT_PNCHEVNT_SKEY BIGINT,
        END_PNCHEVNT_SKEY BIGINT,
        STRT_DSRC_SKEY BIGINT,
        END_DSRC_SKEY BIGINT,
        DSRC_SKEY BIGINT,
        EMPSTAT_SKEY BIGINT,
        AGE_NBR BIGINT,
        TENURE_MO_NBR BIGINT,
        DFLT_PAY_RULE_SKEY BIGINT,
        DFLT_WRK_RULE_SWT INT,
        DFLT_WRK_RULE_SKEY BIGINT,
        WRK_RULE_SKEY BIGINT,
        MONEY_AMT DECIMAL(16, 6),
        DRTN_AMT DECIMAL(16, 6),
        CORE_AMT DECIMAL(16, 6),
        NON_CORE_AMT DECIMAL(16, 6),
        DRTN_DIFF_AMT DECIMAL(16, 6),
        DRTN_HRS DECIMAL(16, 6),
        CORE_HRS DECIMAL(16, 6),
        NON_CORE_HRS DECIMAL(16, 6),
        LOCKED_SWT INT,
        GRP_SCHD_SKEY BIGINT,
        UPDT_DTM TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-raw-p1-gcs-gbl/labor_mgmt/wfa_tdtl_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TDTL_SKEY)
--*****  Creating table:  "WFA_TDTL_PREV_PRE" , ***** Creating table: "WFA_TDTL_PREV_PRE"
use dev_empl_protected;

CREATE TABLE
    raw_WFA_TDTL_PREV_PRE (
        TDTL_SKEY DECIMAL(20, 0) not null,
        TDTL_ID BIGINT,
        TSITEM_SKEY DECIMAL(20, 0),
        RECORDED_DAT TIMESTAMP,
        RECORDED_FOR_DAT TIMESTAMP,
        ADJ_SWT INT,
        EDT_SWT INT,
        EMP_SKEY BIGINT,
        STRT_DTM TIMESTAMP,
        END_DTM TIMESTAMP,
        UNSCHD_STRT_DTM TIMESTAMP,
        UNSCHD_END_DTM TIMESTAMP,
        STRT_TZONE_SKEY BIGINT,
        LBRACCT_SKEY BIGINT,
        HM_LBRACCT_SWT INT,
        HM_LBRACCT_SKEY BIGINT,
        FROM_LBRACCT_SKEY BIGINT,
        PAYCD_SKEY BIGINT,
        CORE_HRS_SWT INT,
        FROM_PAYCD_SKEY BIGINT,
        SUPV_SKEY BIGINT,
        PRI_JOB_SKEY BIGINT,
        JOB_SKEY BIGINT,
        PRI_ORG_SKEY BIGINT,
        ORG_SKEY BIGINT,
        PAYPER_SKEY BIGINT,
        HDAY_SKEY BIGINT,
        STRT_PNCHEVNT_SKEY BIGINT,
        END_PNCHEVNT_SKEY BIGINT,
        STRT_DSRC_SKEY BIGINT,
        END_DSRC_SKEY BIGINT,
        DSRC_SKEY BIGINT,
        EMPSTAT_SKEY BIGINT,
        AGE_NBR BIGINT,
        TENURE_MO_NBR BIGINT,
        DFLT_PAY_RULE_SKEY BIGINT,
        DFLT_WRK_RULE_SWT INT,
        DFLT_WRK_RULE_SKEY BIGINT,
        WRK_RULE_SKEY BIGINT,
        MONEY_AMT DECIMAL(16, 6),
        DRTN_AMT DECIMAL(16, 6),
        CORE_AMT DECIMAL(16, 6),
        NON_CORE_AMT DECIMAL(16, 6),
        DRTN_DIFF_AMT DECIMAL(16, 6),
        DRTN_HRS DECIMAL(16, 6),
        CORE_HRS DECIMAL(16, 6),
        NON_CORE_HRS DECIMAL(16, 6),
        LOCKED_SWT INT,
        GRP_SCHD_SKEY BIGINT
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-raw-p1-gcs-gbl/labor_mgmt/wfa_tdtl_prev_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TDTL_SKEY)
--*****  Creating table:  "WFA_ORG_PRE" , ***** Creating table: "WFA_ORG_PRE"
use dev_raw;

CREATE TABLE
    WFA_ORG_PRE (
        ORG_ID BIGINT not null,
        ORG_SKEY BIGINT,
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
        UPDT_DTM TIMESTAMP,
        REC_ACTV_SWT INT,
        REC_EXP_DTM TIMESTAMP,
        LBRLVL_ID BIGINT,
        LBRLVL_DISP_NBR BIGINT,
        LBRLVL_NAM STRING,
        LBRLVL_ENTRY_ID BIGINT,
        LBRLVL_ENTRY_NAM STRING,
        LBRLVL_ENTRY_DES STRING,
        LBRLVL_ENTRY_ACTV_SWT INT
    ) USING delta LOCATION 'gs://petm-bdpl-dev-raw-p1-gcs-gbl/labor_mgmt/wfa_org_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (ORG_ID)
--*****  Creating table:  "WFA_PAYCD" , ***** Creating table: "WFA_PAYCD"
use dev_legacy;

CREATE TABLE
    WFA_PAYCD (
        PAYCD_ID BIGINT not null,
        PAYCD_SKEY BIGINT not null,
        PAYCD_NAM STRING,
        PAYCD_TYP_COD STRING,
        PAYCD_WAGE_ADD_AMT DECIMAL(16, 6),
        PAYCD_WAGE_MLT_AMT DECIMAL(16, 6),
        PAYCD_MONEY_SWT INT,
        PAYCD_TOT_SWT INT,
        PAYCD_EXCUSED_SWT INT,
        PAYCD_OT_SWT INT,
        PAYCD_CONSEC_OT_SWT INT,
        PAYCD_VISIBLE_SWT INT,
        PAYCAT_NAM STRING,
        CORE_HRS_SWT INT,
        UPDATE_DT TIMESTAMP,
        LOAD_DT TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-refine-p1-gcs-gbl/labor_mgmt/wfa_paycd' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (PAYCD_ID)
--*****  Creating table:  "WFA_PAYCD_PRE" , ***** Creating table: "WFA_PAYCD_PRE"
use dev_raw;

CREATE TABLE
    WFA_PAYCD_PRE (
        PAYCD_ID BIGINT not null,
        PAYCD_SKEY BIGINT,
        PAYCD_NAM STRING,
        PAYCD_TYP_COD STRING,
        PAYCD_WAGE_ADD_AMT DECIMAL(16, 6),
        PAYCD_WAGE_MLT_AMT DECIMAL(16, 6),
        PAYCD_MONEY_SWT INT,
        PAYCD_TOT_SWT INT,
        PAYCD_EXCUSED_SWT INT,
        PAYCD_OT_SWT INT,
        PAYCD_CONSEC_OT_SWT INT,
        PAYCD_VISIBLE_SWT INT,
        PAYCAT_NAM STRING,
        CORE_HRS_SWT INT,
        UPDT_DTM TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-raw-p1-gcs-gbl/labor_mgmt/wfa_paycd_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (PAYCD_ID)
--*****  Creating table:  "WFA_EMP" , ***** Creating table: "WFA_EMP"
use dev_legacy;

CREATE TABLE
    WFA_EMP (
        EMP_ID BIGINT not null,
        CO_HIRE_DAT TIMESTAMP,
        MGR_SIGNOFF_DAT TIMESTAMP,
        PRSN_NBR_TXT STRING,
        SR_RANK_DAT TIMESTAMP,
        DFLT_TZONE_SKEY BIGINT,
        DFLT_TZONE_DES STRING,
        DVC_GRP_NAM STRING,
        HM_LBRACCT_SKEY BIGINT,
        EMP_HM_LBRACCT_EFF_DAT TIMESTAMP,
        EMP_HM_LBRACCT_EXP_DAT TIMESTAMP,
        EMPSTAT_SKEY BIGINT,
        EMP_EMPSTAT_EFF_DAT TIMESTAMP,
        EMP_EMPSTAT_EXP_DAT TIMESTAMP,
        BADGE_COD STRING,
        EMP_BADGE_EFF_DAT TIMESTAMP,
        EMP_BADGE_EXP_DAT TIMESTAMP,
        EM_MNR_SWT INT,
        UPDATE_DT TIMESTAMP,
        LOAD_DT TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-refine-p1-gcs-gbl/labor_mgmt/wfa_emp' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (EMP_ID)
--*****  Creating table:  "WFA_EMP_PRE" , ***** Creating table: "WFA_EMP_PRE"
use dev_raw;

CREATE TABLE
    WFA_EMP_PRE (
        EMP_ID BIGINT not null,
        EMP_SKEY BIGINT,
        CO_HIRE_DAT TIMESTAMP,
        MGR_SIGNOFF_DAT TIMESTAMP,
        PRSN_NBR_TXT STRING,
        SR_RANK_DAT TIMESTAMP,
        DFLT_TZONE_SKEY BIGINT,
        DFLT_TZONE_DES STRING,
        DVC_GRP_NAM STRING,
        HM_LBRACCT_SKEY BIGINT,
        EMP_HM_LBRACCT_EFF_DAT TIMESTAMP,
        EMP_HM_LBRACCT_EXP_DAT TIMESTAMP,
        EMPSTAT_SKEY BIGINT,
        EMP_EMPSTAT_EFF_DAT TIMESTAMP,
        EMP_EMPSTAT_EXP_DAT TIMESTAMP,
        BADGE_COD STRING,
        EMP_BADGE_EFF_DAT TIMESTAMP,
        EMP_BADGE_EXP_DAT TIMESTAMP,
        EM_MNR_SWT INT,
        UPDT_DTM TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-raw-p1-gcs-gbl/labor_mgmt/wfa_emp_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (EMP_ID)
--*****  Creating table:  "WFA_ORG" , ***** Creating table: "WFA_ORG"
use dev_legacy;

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
    ) USING delta LOCATION 'gs://petm-bdpl-dev-refine-p1-gcs-gbl/labor_mgmt/wfa_org' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (ORG_ID)
--*****  Creating table:  "WFA_LBRACCT" , ***** Creating table: "WFA_LBRACCT"
use dev_legacy;

CREATE TABLE
    WFA_LBRACCT (
        LBRACCT_ID INT not null,
        LBRACCT_SKEY INT,
        LBRACCT_FULL_ID STRING,
        LBRACCT_FULL_NAM STRING,
        LBRLVL1_NAM STRING,
        LBRACCT1_ID INT,
        LBRACCT1_NAM STRING,
        LBRACCT1_DES STRING,
        LBRLVL2_NAM STRING,
        LBRACCT2_ID INT,
        LBRACCT2_NAM STRING,
        LBRACCT2_DES STRING,
        LBRLVL3_NAM STRING,
        LBRACCT3_ID INT,
        LBRACCT3_NAM STRING,
        LBRACCT3_DES STRING,
        LBRLVL4_NAM STRING,
        LBRACCT4_ID INT,
        LBRACCT4_NAM STRING,
        LBRACCT4_DES STRING,
        LBRLVL5_NAM STRING,
        LBRACCT5_ID INT,
        LBRACCT5_NAM STRING,
        LBRACCT5_DES STRING,
        LBRLVL6_NAM STRING,
        LBRACCT6_ID INT,
        LBRACCT6_NAM STRING,
        LBRACCT6_DES STRING,
        LBRLVL7_NAM STRING,
        LBRACCT7_ID INT,
        LBRACCT7_NAM STRING,
        LBRACCT7_DES STRING,
        UPDT_DTM TIMESTAMP,
        SRC_COD STRING,
        SRC_SKEY INT,
        TENANT_SKEY INT,
        UPDATE_DT TIMESTAMP,
        LOAD_DT TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-refine-p1-gcs-gbl/labor_mgmt/wfa_lbracct' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON RANDOM
--*****  Creating table:  "WFA_LBRACCT_PRE" , ***** Creating table: "WFA_LBRACCT_PRE"
use dev_raw;

CREATE TABLE
    WFA_LBRACCT_PRE (
        LBRACCT_ID INT not null,
        LBRACCT_SKEY INT,
        LBRACCT_FULL_ID STRING,
        LBRACCT_FULL_NAM STRING,
        LBRLVL1_NAM STRING,
        LBRACCT1_ID INT,
        LBRACCT1_NAM STRING,
        LBRACCT1_DES STRING,
        LBRLVL2_NAM STRING,
        LBRACCT2_ID INT,
        LBRACCT2_NAM STRING,
        LBRACCT2_DES STRING,
        LBRLVL3_NAM STRING,
        LBRACCT3_ID INT,
        LBRACCT3_NAM STRING,
        LBRACCT3_DES STRING,
        LBRLVL4_NAM STRING,
        LBRACCT4_ID INT,
        LBRACCT4_NAM STRING,
        LBRACCT4_DES STRING,
        LBRLVL5_NAM STRING,
        LBRACCT5_ID INT,
        LBRACCT5_NAM STRING,
        LBRACCT5_DES STRING,
        LBRLVL6_NAM STRING,
        LBRACCT6_ID INT,
        LBRACCT6_NAM STRING,
        LBRACCT6_DES STRING,
        LBRLVL7_NAM STRING,
        LBRACCT7_ID INT,
        LBRACCT7_NAM STRING,
        LBRACCT7_DES STRING,
        UPDT_DTM TIMESTAMP,
        SRC_COD STRING,
        SRC_SKEY INT,
        TENANT_SKEY INT
    ) USING delta LOCATION 'gs://petm-bdpl-dev-raw-p1-gcs-gbl/labor_mgmt/wfa_lbracct_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON RANDOM
--*****  Creating table:  "WFA_FCST_SLS_DEPT" , ***** Creating table: "WFA_FCST_SLS_DEPT"
use dev_legacy;

CREATE TABLE
    WFA_FCST_SLS_DEPT (
        DAY_DT TIMESTAMP not null,
        ORG_ID BIGINT not null,
        LOCATION_ID INT not null,
        WFA_BUSN_AREA_ID SMALLINT,
        WFA_BUSN_AREA_DESC STRING,
        WFA_DEPT_ID SMALLINT,
        WFA_DEPT_DESC STRING,
        ACTL_SLS_AMT DECIMAL(16, 6),
        ACTL_SALON_ITEMS_CNT DECIMAL(16, 6),
        ACTL_REG_TRANS_CNT DECIMAL(16, 6),
        ACTL_CUST_INTERACT_CNT DECIMAL(16, 6),
        ACTL_LIVE_ITEMS_RECVD_CNT DECIMAL(16, 6),
        ACTL_REG_ITEMS_CNT DECIMAL(16, 6),
        ACTL_RESTOCK_CNT DECIMAL(16, 6),
        FCST_ORIG_SLS_AMT DECIMAL(16, 6),
        FCST_SLS_AMT DECIMAL(16, 6),
        FCST_ORIG_SALON_ITEMS_CNT DECIMAL(16, 6),
        FCST_SALON_ITEMS_CNT DECIMAL(16, 6),
        FCST_ORIG_REG_TRANS_CNT DECIMAL(16, 6),
        FCST_REG_TRANS_CNT DECIMAL(16, 6),
        FCST_ORIG_CUST_INTERACT_CNT DECIMAL(16, 6),
        FCST_CUST_INTERACT_CNT DECIMAL(16, 6),
        FCST_ORIG_LIVE_ITEMS_RECVD_CNT DECIMAL(16, 6),
        FCST_LIVE_ITEMS_RECVD_CNT DECIMAL(16, 6),
        FCST_ORIG_REG_ITEMS_CNT DECIMAL(16, 6),
        FCST_REG_ITEMS_CNT DECIMAL(16, 6),
        FCST_ORIG_RESTOCK_CNT DECIMAL(16, 6),
        FCST_RESTOCK_CNT DECIMAL(16, 6),
        BUD_AMT DECIMAL(16, 6),
        BUD_HRS DECIMAL(16, 6),
        BUD_RATE_AMT DECIMAL(16, 6),
        BUD_SLS_AMT DECIMAL(16, 6),
        WEEK_DT TIMESTAMP,
        UPDATE_DT TIMESTAMP,
        LOAD_DT TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-refine-p1-gcs-gbl/labor_mgmt/wfa_fcst_sls_dept' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (ORG_ID)
--*****  Creating table:  "WFA_FCST_SLS_TASK" , ***** Creating table: "WFA_FCST_SLS_TASK"
use dev_legacy;

CREATE TABLE
    WFA_FCST_SLS_TASK (
        DAY_DT TIMESTAMP not null,
        ORG_ID BIGINT not null,
        LOCATION_ID INT not null,
        WFA_BUSN_AREA_ID SMALLINT,
        WFA_BUSN_AREA_DESC STRING,
        WFA_DEPT_ID SMALLINT,
        WFA_DEPT_DESC STRING,
        WFA_TASK_ID SMALLINT,
        WFA_TASK_DESC STRING,
        ACTL_AMT DECIMAL(16, 6),
        ACTL_HRS DECIMAL(16, 6),
        ACTL_RATE_AMT DECIMAL(16, 6),
        ACTL_REG_AMT DECIMAL(16, 6),
        ACTL_REG_HRS DECIMAL(16, 6),
        ACTL_OT_AMT DECIMAL(16, 6),
        ACTL_OT_HRS DECIMAL(16, 6),
        ACTL_OTH_AMT DECIMAL(16, 6),
        ACTL_OTH_HRS DECIMAL(16, 6),
        ACTL_NPRD_AMT DECIMAL(16, 6),
        ACTL_NPRD_HRS DECIMAL(16, 6),
        ACTL_TRNG_AMT DECIMAL(16, 6),
        ACTL_TRNG_HRS DECIMAL(16, 6),
        ACTL_UNK_AMT DECIMAL(16, 6),
        ACTL_UNK_HRS DECIMAL(16, 6),
        SCHD_AMT DECIMAL(16, 6),
        SCHD_HRS DECIMAL(16, 6),
        SCHD_RATE_AMT DECIMAL(16, 6),
        SCHD_REG_AMT DECIMAL(16, 6),
        SCHD_REG_HRS DECIMAL(16, 6),
        SCHD_OT_AMT DECIMAL(16, 6),
        SCHD_OT_HRS DECIMAL(16, 6),
        SCHD_OTH_AMT DECIMAL(16, 6),
        SCHD_OTH_HRS DECIMAL(16, 6),
        SCHD_NPRD_AMT DECIMAL(16, 6),
        SCHD_NPRD_HRS DECIMAL(16, 6),
        SCHD_TRNG_AMT DECIMAL(16, 6),
        SCHD_TRNG_HRS DECIMAL(16, 6),
        SCHD_UNK_AMT DECIMAL(16, 6),
        SCHD_UNK_HRS DECIMAL(16, 6),
        FCST_AMT DECIMAL(16, 6),
        FCST_HRS DECIMAL(16, 6),
        FCST_RATE_AMT DECIMAL(16, 6),
        WEEK_DT TIMESTAMP,
        UPDATE_DT TIMESTAMP,
        LOAD_DT TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-refine-p1-gcs-gbl/labor_mgmt/wfa_fcst_sls_task' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (ORG_ID)
--*****  Creating table:  "WFA_TSCHD_DIFF_PRE" , ***** Creating table: "WFA_TSCHD_DIFF_PRE"
use dev_empl_protected;

CREATE TABLE
    raw_WFA_TSCHD_DIFF_PRE (
        TSCHD_ID BIGINT not null,
        TSCHD_DAT TIMESTAMP not null,
        EMP_SKEY BIGINT,
        STRT_DTM TIMESTAMP,
        END_DTM TIMESTAMP,
        PAYCD_SKEY BIGINT,
        CORE_HRS_SWT INT,
        PRI_ORG_SKEY BIGINT,
        ORG_SKEY BIGINT,
        SHIFT_ID BIGINT,
        MONEY_AMT DECIMAL(16, 6),
        DRTN_AMT DECIMAL(16, 6),
        CORE_AMT DECIMAL(16, 6),
        NON_CORE_AMT DECIMAL(16, 6),
        DRTN_HRS DECIMAL(16, 6),
        CORE_HRS DECIMAL(16, 6),
        NON_CORE_HRS DECIMAL(16, 6)
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-raw-p1-gcs-gbl/labor_mgmt/wfa_tschd_diff_pre' TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (TSCHD_ID)
--*****  Creating table:  "WFA_TSCHD" , ***** Creating table: "WFA_TSCHD"
use dev_empl_protected;

CREATE TABLE
    legacy_WFA_TSCHD (
        DAY_DT TIMESTAMP not null,
        TSCHD_ID BIGINT not null,
        STRT_DTM TIMESTAMP,
        END_DTM TIMESTAMP,
        LOCATION_ID INT,
        EMPLOYEE_ID BIGINT,
        WFA_BUSN_AREA_ID SMALLINT,
        WFA_BUSN_AREA_DESC STRING,
        WFA_DEPT_ID SMALLINT,
        WFA_DEPT_DESC STRING,
        WFA_TASK_ID SMALLINT,
        WFA_TASK_DESC STRING,
        PRI_LOCATION_ID INT,
        PRI_WFA_BUSN_AREA_ID SMALLINT,
        PRI_WFA_BUSN_AREA_DESC STRING,
        PRI_WFA_DEPT_ID SMALLINT,
        PRI_WFA_DEPT_DESC STRING,
        PRI_WFA_TASK_ID SMALLINT,
        PRI_WFA_TASK_DESC STRING,
        PAYCD_ID BIGINT,
        CORE_HRS_SWT INT,
        SHIFT_ID BIGINT,
        MONEY_AMT DECIMAL(16, 6),
        DRTN_AMT DECIMAL(16, 6),
        CORE_AMT DECIMAL(16, 6),
        NON_CORE_AMT DECIMAL(16, 6),
        DRTN_HRS DECIMAL(16, 6),
        CORE_HRS DECIMAL(16, 6),
        NON_CORE_HRS DECIMAL(16, 6),
        WEEK_DT TIMESTAMP,
        UPDATE_DT TIMESTAMP,
        LOAD_DT TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-nzlegacy-p1-gcs-gbl/labor_mgmt/wfa_tschd';

--DISTRIBUTE ON (TSCHD_ID)
--*****  Creating table:  "WFA_TDTL" , ***** Creating table: "WFA_TDTL"
use dev_empl_protected;

CREATE TABLE
    legacy_WFA_TDTL (
        DAY_DT TIMESTAMP ,
        TDTL_ID BIGINT,
        STRT_DTM TIMESTAMP,
        END_DTM TIMESTAMP,
        LOCATION_ID INT,
        EMPLOYEE_ID BIGINT,
        WFA_BUSN_AREA_ID SMALLINT,
        WFA_BUSN_AREA_DESC STRING,
        WFA_DEPT_ID SMALLINT,
        WFA_DEPT_DESC STRING,
        WFA_TASK_ID SMALLINT,
        WFA_TASK_DESC STRING,
        PRI_LOCATION_ID INT,
        PRI_WFA_BUSN_AREA_ID SMALLINT,
        PRI_WFA_BUSN_AREA_DESC STRING,
        PRI_WFA_DEPT_ID SMALLINT,
        PRI_WFA_DEPT_DESC STRING,
        PRI_WFA_TASK_ID SMALLINT,
        PRI_WFA_TASK_DESC STRING,
        TSITEM_ID BIGINT,
        RECORDED_FOR_DAT TIMESTAMP,
        ADJ_SWT INT,
        EDT_SWT INT,
        EMP_SKEY BIGINT,
        UNSCHD_STRT_DTM TIMESTAMP,
        UNSCHD_END_DTM TIMESTAMP,
        STRT_TZONE_SKEY BIGINT,
        LBRACCT_SKEY BIGINT,
        LBRACCT_ID BIGINT,
        HM_LBRACCT_SWT INT,
        HM_LBRACCT_SKEY BIGINT,
        HM_LBRACCT_ID BIGINT,
        FROM_LBRACCT_SKEY BIGINT,
        FROM_LBRACCT_ID BIGINT,
        PAYCD_SKEY BIGINT,
        PAYCD_ID BIGINT,
        CORE_HRS_SWT INT,
        FROM_PAYCD_SKEY BIGINT,
        FROM_PAYCD_ID BIGINT,
        SUPV_SKEY BIGINT,
        PRI_JOB_SKEY BIGINT,
        JOB_SKEY BIGINT,
        PRI_ORG_SKEY BIGINT,
        ORG_SKEY BIGINT,
        PAYPER_SKEY BIGINT,
        HDAY_SKEY BIGINT,
        STRT_PNCHEVNT_SKEY BIGINT,
        END_PNCHEVNT_SKEY BIGINT,
        STRT_DSRC_SKEY BIGINT,
        END_DSRC_SKEY BIGINT,
        DSRC_SKEY BIGINT,
        EMPSTAT_SKEY BIGINT,
        AGE_NBR BIGINT,
        TENURE_MO_NBR BIGINT,
        DFLT_PAY_RULE_SKEY BIGINT,
        DFLT_WRK_RULE_SWT INT,
        DFLT_WRK_RULE_SKEY BIGINT,
        WRK_RULE_SKEY BIGINT,
        MONEY_AMT DECIMAL(16, 6),
        DRTN_AMT DECIMAL(16, 6),
        CORE_AMT DECIMAL(16, 6),
        NON_CORE_AMT DECIMAL(16, 6),
        DRTN_DIFF_AMT DECIMAL(16, 6),
        DRTN_HRS DECIMAL(16, 6),
        CORE_HRS DECIMAL(16, 6),
        NON_CORE_HRS DECIMAL(16, 6),
        LOCKED_SWT INT,
        GRP_SCHD_SKEY BIGINT,
        WORK_LOCATION_ID BIGINT,
        WEEK_DT TIMESTAMP,
        UPDATE_DT TIMESTAMP,
        LOAD_DT TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-nzlegacy-p1-gcs-gbl/labor_mgmt/wfa_tdtl';

--DISTRIBUTE ON (TDTL_ID)
--*****  Creating table:  "WFA_TDTL_DIFF_PRE" , ***** Creating table: "WFA_TDTL_DIFF_PRE"
use dev_empl_protected;

CREATE TABLE
    raw_WFA_TDTL_DIFF_PRE (
        TDTL_SKEY DECIMAL(20, 0) not null,
        TDTL_ID BIGINT,
        TSITEM_SKEY DECIMAL(20, 0),
        RECORDED_DAT TIMESTAMP,
        RECORDED_FOR_DAT TIMESTAMP,
        ADJ_SWT INT,
        EDT_SWT INT,
        EMP_SKEY BIGINT,
        STRT_DTM TIMESTAMP,
        END_DTM TIMESTAMP,
        UNSCHD_STRT_DTM TIMESTAMP,
        UNSCHD_END_DTM TIMESTAMP,
        STRT_TZONE_SKEY BIGINT,
        LBRACCT_SKEY BIGINT,
        HM_LBRACCT_SWT INT,
        HM_LBRACCT_SKEY BIGINT,
        FROM_LBRACCT_SKEY BIGINT,
        PAYCD_SKEY BIGINT,
        CORE_HRS_SWT INT,
        FROM_PAYCD_SKEY BIGINT,
        SUPV_SKEY BIGINT,
        PRI_JOB_SKEY BIGINT,
        JOB_SKEY BIGINT,
        PRI_ORG_SKEY BIGINT,
        ORG_SKEY BIGINT,
        PAYPER_SKEY BIGINT,
        HDAY_SKEY BIGINT,
        STRT_PNCHEVNT_SKEY BIGINT,
        END_PNCHEVNT_SKEY BIGINT,
        STRT_DSRC_SKEY BIGINT,
        END_DSRC_SKEY BIGINT,
        DSRC_SKEY BIGINT,
        EMPSTAT_SKEY BIGINT,
        AGE_NBR BIGINT,
        TENURE_MO_NBR BIGINT,
        DFLT_PAY_RULE_SKEY BIGINT,
        DFLT_WRK_RULE_SWT INT,
        DFLT_WRK_RULE_SKEY BIGINT,
        WRK_RULE_SKEY BIGINT,
        MONEY_AMT DECIMAL(16, 6),
        DRTN_AMT DECIMAL(16, 6),
        CORE_AMT DECIMAL(16, 6),
        NON_CORE_AMT DECIMAL(16, 6),
        DRTN_DIFF_AMT DECIMAL(16, 6),
        DRTN_HRS DECIMAL(16, 6),
        CORE_HRS DECIMAL(16, 6),
        NON_CORE_HRS DECIMAL(16, 6),
        LOCKED_SWT INT,
        GRP_SCHD_SKEY BIGINT
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-raw-p1-gcs-gbl/labor_mgmt/wfa_tdtl_diff_pre';

--DISTRIBUTE ON (TDTL_SKEY)
--*****  Creating table:  "EMPLOYEE_PROFILE" , ***** Creating table: "EMPLOYEE_PROFILE"
use dev_empl_protected;

CREATE TABLE
    legacy_EMPLOYEE_PROFILE (
        EMPLOYEE_ID INT not null,
        EMPL_FIRST_NAME STRING,
        EMPL_MIDDLE_NAME STRING,
        EMPL_LAST_NAME STRING,
        EMPL_BIRTH_DT TIMESTAMP,
        GENDER_CD STRING,
        PS_MARITAL_STATUS_CD STRING,
        ETHNIC_GROUP_ID STRING,
        EMPL_ADDR_1 STRING,
        EMPL_ADDR_2 STRING,
        EMPL_CITY STRING,
        EMPL_STATE STRING,
        EMPL_PROVINCE STRING,
        EMPL_ZIPCODE STRING,
        COUNTRY_CD STRING,
        EMPL_HOME_PHONE STRING,
        EMPL_EMAIL_ADDR STRING,
        EMPL_LOGIN_ID STRING,
        BADGE_NBR STRING,
        EMPL_STATUS_CD STRING,
        STATUS_CHG_DT TIMESTAMP,
        FULLPT_FLAG STRING,
        FULLPT_CHG_DT TIMESTAMP,
        EMPL_TYPE_CD STRING,
        PS_REG_TEMP_CD STRING,
        EMPL_CATEGORY_CD STRING,
        EMPL_GROUP_CD STRING,
        EMPL_SUBGROUP_CD STRING,
        EMPL_HIRE_DT TIMESTAMP,
        EMPL_REHIRE_DT TIMESTAMP,
        EMPL_TERM_DT TIMESTAMP,
        TERM_REASON_CD STRING,
        EMPL_SENORITY_DT TIMESTAMP,
        PS_ACTION_DT TIMESTAMP,
        PS_ACTION_CD STRING,
        PS_ACTION_REASON_CD STRING,
        LOCATION_ID INT,
        LOCATION_CHG_DT TIMESTAMP,
        STORE_NBR INT,
        STORE_DEPT_NBR STRING,
        COMPANY_ID INT,
        PS_PERSONNEL_AREA_ID STRING,
        PS_PERSONNEL_SUBAREA_ID STRING,
        PS_DEPT_CD STRING,
        PS_DEPT_CHG_DT TIMESTAMP,
        PS_POSITION_ID INT,
        POSITION_CHG_DT TIMESTAMP,
        PS_SUPERVISOR_ID INT,
        JOB_CODE INT,
        JOB_CODE_CHG_DT TIMESTAMP,
        EMPL_JOB_ENTRY_DT TIMESTAMP,
        PS_GRADE_ID SMALLINT,
        EMPL_STD_BONUS_PCT DECIMAL(5, 2),
        EMPL_OVR_BONUS_PCT DECIMAL(5, 2),
        EMPL_RATING DECIMAL(5, 2),
        PAY_RATE_CHG_DT TIMESTAMP,
        PS_PAYROLL_AREA_CD STRING,
        PS_TAX_COMPANY_CD STRING,
        PS_COMP_FREQ_CD STRING,
        COMP_RATE_AMT DECIMAL(15, 2),
        ANNUAL_RATE_LOC_AMT DECIMAL(15, 2),
        HOURLY_RATE_LOC_AMT DECIMAL(12, 2),
        CURRENCY_ID STRING,
        EXCH_RATE_PCT DECIMAL(9, 6),
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-dev-empl-protected-nzlegacy-p1-gcs-gbl/labor_mgmt/employee_profile';

--DISTRIBUTE ON (EMPLOYEE_ID)