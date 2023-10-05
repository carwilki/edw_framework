insert into work.pii_dynamic_view_control
select 
"cust_sensitive",
"legacy_e_res_pets",
"legacy",
"e_res_pets",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');


insert into work.pii_dynamic_view_control
select 
"cust_sensitive",
"legacy_e_res_requests",
"legacy",
"e_res_requests",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');

insert into work.pii_dynamic_view_control
select 
"empl_protected",
"legacy_ic_wc_claims",
"legacy",
"ic_wc_claims",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');
