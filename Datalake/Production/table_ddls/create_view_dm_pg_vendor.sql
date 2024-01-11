-- Databricks notebook source
create view legacy.dm_pg_vendor as
select
  vendor_id,
  vendor_name,
  purch_group_id,
  parent_vendor_id,
  parent_vendor_name,
  vendor_team_member,
  vendor_team_lead,
  vendor_mgt_lead
from
  refine.udf_usr_pg_vendor
