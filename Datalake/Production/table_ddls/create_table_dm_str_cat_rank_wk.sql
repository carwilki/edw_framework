-- Databricks notebook source
CREATE TABLE legacy.DM_STR_CAT_RANK_WK
(
	SAP_CATEGORY_ID INT,
	LOCATION_ID INT,
	STR_RANK_SLS_DLR string,
	STR_RANK_SLS_QTY string,
	STR_RANK_GM_DLR string
)
using delta
location "gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/IT/dm_str_cat_rank_wk"
