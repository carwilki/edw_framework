-- Databricks notebook source
CREATE TABLE legacy.USR_STORE_TYPE
(
	LOCATION_ID INT,
	STORENBR INT,
	STORE_TYPE string
)
using delta
location "gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/usr_store_type"
