-- Databricks notebook source
insert into work.pii_dynamic_view_control
select 
"cust_sensitive",
"legacy_pet_training_reservation",
"legacy",
"pet_training_reservation",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');


insert into work.pii_dynamic_view_control
select 
"cust_sensitive",
"legacy_training_customer",
"legacy",
"training_customer",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');


insert into work.pii_dynamic_view_control
select 
"empl_sensitive",
"refine_HONORARY_DESIGNEE",
"refine",
"HONORARY_DESIGNEE",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');

