-- Databricks notebook source
create view legacy.usr_dat_names as
select
  STORE_NBR,
  PHO_QE_Manager,
  PHO_Technical_Manager,
  DAT,
  Manager_1,
  Manager_2,
  Manager_3
from
  refine.udf_usr_dat_names
