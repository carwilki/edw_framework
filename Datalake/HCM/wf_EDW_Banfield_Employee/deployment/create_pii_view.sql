-- Databricks notebook source
insert into work.pii_dynamic_view_control
select 
"empl_sensitive",
"refine_banfield_employee",
"refine",
"banfield_employee",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');

 

insert into pii_metadata.pii_metadata_store
select * from pii_metadata.pii_metadata_store_qa
where table_name='refine_banfield_employee';
