Insert into work.pii_dynamic_view_control
select
"employee_protected",
"legacy_WFA_TSCHD",
"legacy",
"WFA_TSCHD",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');


insert into work.pii_dynamic_view_control
select
"empl_protected",
"legacy_WFA_TDTL",
"legacy",
"WFA_TDTL",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');
 



insert into work.pii_dynamic_view_control
select
"empl_protected",
"legacy_EMPLOYEE_PROFILE",
"legacy",
"EMPLOYEE_PROFILE",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');