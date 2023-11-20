insert into work.pii_dynamic_view_control
select 
"empl_sensitive",
"legacy_GS_TRAVEL",
"legacy",
"GS_TRAVEL",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');