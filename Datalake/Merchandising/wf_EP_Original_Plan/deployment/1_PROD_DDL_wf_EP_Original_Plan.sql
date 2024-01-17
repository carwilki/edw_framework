




--*****  Creating table:  "EP_PLAN_CLASS_WK" , ***** Creating table: "EP_PLAN_CLASS_WK"


use legacy;
 CREATE TABLE  EP_PLAN_CLASS_WK 
(
 EP_ORG_KEY INT not null

, EP_VERSION                                  STRING                 not null

, SAP_CLASS_ID INT not null

, WEEK_DT                                    DATE                                not null

, FISCAL_WK INT not null

, NET_SALES_AMT                               DECIMAL(24,12) 

, NET_SALES_QTY                               DECIMAL(24,12) 

, NET_SALES_COST_AMT                          DECIMAL(24,12) 

, CLEARANCE_SALES_AMT                         DECIMAL(24,12) 

, PROMO_SALES_AMT                             DECIMAL(24,12) 

, PROMO_FUND_AMT                              DECIMAL(24,12) 

, MARKDOWN_CLEARANCE_AMT                      DECIMAL(24,12) 

, MARKDOWN_PROMO_AMT                          DECIMAL(24,12) 

, OBSOLESCENCE_QTY                            DECIMAL(24,12) 

, OBSOLESCENCE_AMT                            DECIMAL(24,12) 

, PRODUCT_REMOVAL_QTY                         DECIMAL(24,12) 

, PRODUCT_REMOVAL_AMT                         DECIMAL(24,12) 

, SHRINK_QTY                                  DECIMAL(24,12) 

, SHRINK_AMT                                  DECIMAL(24,12) 

, DAMAGES_QTY                                 DECIMAL(24,12) 

, DAMAGES_AMT                                 DECIMAL(24,12) 

, SERVICE_ADJ_AMT                             DECIMAL(24,12) 

, SERVICE_EXPENSE_AMT                         DECIMAL(24,12) 

, ROYALTY_AMT                                 DECIMAL(24,12) 

, OUTBOUND_FREIGHT_AMT                        DECIMAL(24,12) 

, OTHER_ADJ_AMT                               DECIMAL(24,12) 

, COMP_BASE_SALES_AMT                         DECIMAL(24,12) 

, COMP_SALES_AMT                              DECIMAL(24,12) 

, COMP_SALES_QTY                              DECIMAL(24,12) 

, NON_COMP_SALES_AMT                          DECIMAL(24,12) 

, COMP_MARGIN_AMT                             DECIMAL(24,12) 

, RX_COMMISSION_AMT                           DECIMAL(24,12) 

, CLEARANCE_FUND_AMT                          DECIMAL(24,12) 

, DC_COST_AMT                                 DECIMAL(24,12) 

, DEFECT_ALLOWANCE_AMT                        DECIMAL(24,12) 

, CASH_DISC_AMT                               DECIMAL(24,12) 

, START_INV_QTY                               DECIMAL(24,12) 

, START_INV_AMT                               DECIMAL(24,12) 

, END_INV_NORM_QTY                            DECIMAL(24,12) 

, END_INV_NORM_AMT                            DECIMAL(24,12) 

, END_INV_QTY                                 DECIMAL(24,12) 

, END_INV_AMT                                 DECIMAL(24,12) 

, ADJ_INV_QTY                                 DECIMAL(24,12) 

, ADJ_INV_AMT                                 DECIMAL(24,12) 

, MOVEMENT_RECEIPT_QTY                        DECIMAL(24,12) 

, MOVEMENT_RECEIPT_AMT                        DECIMAL(24,12) 

, EP_UPDATE_TSTMP                             TIMESTAMP                            not null

, UPDATE_TSTMP                                TIMESTAMP                            not null

, LOAD_TSTMP                                  TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/Merchandising/ep_plan_class_wk';

--DISTRIBUTE ON (EP_VERSION, SAP_CLASS_ID)

--ORGANIZE   ON (WEEK_DT)







--*****  Creating table:  "EP_PF_FT_PL_OP1_000_PRE" , ***** Creating table: "EP_PF_FT_PL_OP1_000_PRE"


use raw;
 CREATE TABLE  EP_PF_FT_PL_OP1_000_PRE 
(
 PRODUCT_KEY INT not null

, ORGANIZATION_KEY INT not null

, CALENDAR_KEY INT not null

, PRODUCT_NAME                                STRING                not null

, CALENDAR_NAME                               STRING                not null

, ADJ_UNT_OP                                  DECIMAL(24,12) 

, ADJ_VAL_OP                                  DECIMAL(24,12) 

, BFRX_VAL_OP                                 DECIMAL(24,12) 

, BOP_UNT_OP                                  DECIMAL(24,12) 

, BOP_VAL_OP                                  DECIMAL(24,12) 

, CLRFUND_VAL_OP                              DECIMAL(24,12) 

, COMBSLS_RTL_OP                              DECIMAL(24,12) 

, COMPSLS_RTL_OP                              DECIMAL(24,12) 

, COMPSLS_UNT_OP                              DECIMAL(24,12) 

, DCCOSTS_VAL_OP                              DECIMAL(24,12) 

, DEFECT_VAL_OP                               DECIMAL(24,12) 

, CD_VAL_OP                                   DECIMAL(24,12) 

, EOPNORM_UNT_OP                              DECIMAL(24,12) 

, EOPNORM_VAL_OP                              DECIMAL(24,12) 

, EOP_UNT_OP                                  DECIMAL(24,12) 

, EOP_VAL_OP                                  DECIMAL(24,12) 

, MDCLR_VAL_OP                                DECIMAL(24,12) 

, MDPRO_VAL_OP                                DECIMAL(24,12) 

, NCOMSLS_RTL_OP                              DECIMAL(24,12) 

, OBSO_UNT_OP                                 DECIMAL(24,12) 

, OBSO_VAL_OP                                 DECIMAL(24,12) 

, OTHADJ_VAL_OP                               DECIMAL(24,12) 

, OUTFRT_VAL_OP                               DECIMAL(24,12) 

, PROFUND_VAL_OP                              DECIMAL(24,12) 

, REC_UNT_OP                                  DECIMAL(24,12) 

, REC_VAL_OP                                  DECIMAL(24,12) 

, REMOVAL_UNT_OP                              DECIMAL(24,12) 

, REMOVAL_VAL_OP                              DECIMAL(24,12) 

, ROYS_VAL_OP                                 DECIMAL(24,12) 

, RSLSCLR_VAL_OP                              DECIMAL(24,12) 

, RSLSPRO_VAL_OP                              DECIMAL(24,12) 

, SHRINK_UNT_OP                               DECIMAL(24,12) 

, SHRINK_VAL_OP                               DECIMAL(24,12) 

, TRTLSLS_VAL_OP                              DECIMAL(24,12) 

, TTLSLS_UNT_OP                               DECIMAL(24,12) 

, TTLSLS_VAL_OP                               DECIMAL(24,12) 

, DAMAGES_UNT_OP                              DECIMAL(24,12) 

, DAMAGES_VAL_OP                              DECIMAL(24,12) 

, SERVADJ_VAL_OP                              DECIMAL(24,12) 

, SERVEXP_VAL_OP                              DECIMAL(24,12) 

, COUNTBPRD_OP                                DECIMAL(24,12) 

, DUMMYSTR_OP                                 DECIMAL(24,12) 

, COMPGM_VAL_OP                               DECIMAL(24,12) 

, UPDATE_DATE                                 TIMESTAMP                            not null

, LOAD_TSTMP                                  TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/Merchandising/ep_pf_ft_pl_op1_000_pre';

--DISTRIBUTE ON RANDOM










