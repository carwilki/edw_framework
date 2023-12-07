-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW legacy.location_area AS
-- MAGIC SELECT LOCATION_ID,AREA_ID,CAST(LOC_AREA_EFF_DT AS DATE) LOC_AREA_EFF_DT,CAST(LOC_AREA_END_DT AS DATE) LOC_AREA_END_DT,SQ_FT_AMT FROM refine.sap_location_area
