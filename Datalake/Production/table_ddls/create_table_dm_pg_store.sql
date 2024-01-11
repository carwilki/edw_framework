-- Databricks notebook source
CREATE TABLE legacy.DM_PG_STORE
(
	STORE_NBR INT,
	PURCH_GROUP_ID INT
)
using delta
location "gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/dm_pg_store"
