delete from refine.wm_c_tms_plan a
where exists (
select LOCATION_ID,WM_C_TMS_PLAN_ID from
qa_work.raptor_dataset_src_extra_bd_vs_sf_NZ_UAT_WM_C_TMS_PLAN_10012023_190923 
where a.location_id = location_id and a.wm_c_tms_plan_id = WM_C_TMS_PLAN_ID);