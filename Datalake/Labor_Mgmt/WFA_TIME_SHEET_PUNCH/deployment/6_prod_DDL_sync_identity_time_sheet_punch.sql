--*****  Alter Table Sync Identity:  "WFA_TASK" 

alter table legacy.wfa_task alter column WFA_TASK_ID sync identity

--*****  Alter Table Sync Identity:  "WFA_DEPARTMENT" 
alter table legacy.wfa_department alter column WFA_DEPT_ID sync identity 

--*****  Alter Table Sync Identity:  "WFA_BUSINESS_AREA" 
alter table legacy.wfa_business_area alter column wfa_busn_area_id sync identity
