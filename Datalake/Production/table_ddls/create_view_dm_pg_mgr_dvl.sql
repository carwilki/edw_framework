-- Databricks notebook source
create view legacy.dm_pg_mgr_dvl as
select
  upmd.mgr_id,
  upm.mgr_desc,
  upmd.dvl_id,
  upd.dvl_desc,
  upmd.purch_group_id,
  pg.purch_group_name
from
  refine.udf_usr_pg_mgr_dvl upmd
  left join refine.udf_usr_pg_dvl upd on upd.dvl_id = upmd.dvl_id
  left join refine.udf_usr_pg_mgr upm on upm.mgr_id = upmd.mgr_id
  and upm.dvl_id = upmd.dvl_id
  left join refine.sap_t024_purch_group pg on pg.purch_group_id = upmd.purch_group_id
