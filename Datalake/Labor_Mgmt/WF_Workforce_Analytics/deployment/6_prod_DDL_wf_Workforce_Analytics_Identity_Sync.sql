--*****  Sync Identity  table:  "WFA_TASK" 

alter table legacy.wfa_task alter column WFA_TASK_ID sync identity;

--*****  Sync Identity  table:  "wfa_department" 

alter table legacy.wfa_department alter column WFA_DEPT_ID sync identity ;

--*****  Sync Identity  table:  "wfa_business_area" 

alter table legacy.wfa_business_area alter column wfa_busn_area_id sync identity;