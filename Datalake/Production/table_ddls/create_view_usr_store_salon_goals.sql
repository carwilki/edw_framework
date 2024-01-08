-- Databricks notebook source
create view legacy.usr_store_salon_goals as
select
  STORE_NBR,
  FISCAL_WK,
  SALON_TIER_FSG,
  STYLIST_DOGS_8_HR,
  STYLIST_AVG_APPT_AMT,
  STYLIST_ADD_ON_PERCENT,
  BATHER_DOGS_8_HR,
  BATHER_AVG_APPT_AMT,
  BATHER_ADD_ON_PERCENT,
  MIN_GROOM_AMT,
  SALON_TIER_BB
from
  refine.udf_usr_store_salon_goals
