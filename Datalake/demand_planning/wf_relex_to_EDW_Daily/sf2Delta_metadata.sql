delete from raw.sf2delta_pkey_tstcols where tableName="DP_ACCURACY_DAY";
delete from raw.sf2delta_pkey_tstcols where tableName="DP_DEMAND_DAY";
delete from raw.sf2delta_pkey_tstcols where tableName="DP_FORECAST_WEEK_HIST";
delete from raw.sf2delta_pkey_tstcols where tableName="DP_ORDER_PROJECTION_DAY_HIST";
delete from raw.sf2delta_pkey_tstcols where tableName="DP_ORDER_PROJECTION_WEEK_HIST";
delete from raw.sf2delta_pkey_tstcols where tableName="DP_PRODUCT_LOCATION_SETTINGS_HIST";


insert into raw.sf2delta_pkey_tstcols values("DP_ACCURACY_DAY","DAY_DT|LOCATION_ID|PRODUCT_ID|SOURCE_VENDOR_ID","","UPDATE_TSTMP|LOAD_TSTMP");
insert into raw.sf2delta_pkey_tstcols values("DP_DEMAND_DAY","DAY_DT|LOCATION_ID|PRODUCT_ID|FROM_LOCATION_ID","","UPDATE_TSTMP|LOAD_TSTMP");
insert into raw.sf2delta_pkey_tstcols values("DP_FORECAST_WEEK_HIST","SNAPSHOT_DT|WEEK_DT|LOCATION_ID|PRODUCT_ID|FROM_LOCATION_ID","","UPDATE_TSTMP|LOAD_TSTMP");
insert into raw.sf2delta_pkey_tstcols values("DP_ORDER_PROJECTION_DAY_HIST","SNAPSHOT_DT|DAY_DT|LOCATION_ID|PRODUCT_ID|FROM_LOCATION_ID","","UPDATE_TSTMP|LOAD_TSTMP");
insert into raw.sf2delta_pkey_tstcols values("DP_ORDER_PROJECTION_WEEK_HIST","SNAPSHOT_DT|WEEK_DT|LOCATION_ID|PRODUCT_ID|FROM_LOCATION_ID","","UPDATE_TSTMP|LOAD_TSTMP");
insert into raw.sf2delta_pkey_tstcols values("DP_PRODUCT_LOCATION_SETTINGS_HIST","SNAPSHOT_DT|LOCATION_ID|PRODUCT_ID|SOURCE_VENDOR_ID","","UPDATE_TSTMP|LOAD_TSTMP");

delete from raw.historical_run_details_from_sf where table_name="DP_ACCURACY_DAY";
delete from raw.historical_run_details_from_sf where tableName="DP_DEMAND_DAY";
delete from raw.historical_run_details_from_sf where tableName="DP_FORECAST_WEEK_HIST";
delete from raw.historical_run_details_from_sf where tableName="DP_ORDER_PROJECTION_DAY_HIST";
delete from raw.historical_run_details_from_sf where tableName="DP_ORDER_PROJECTION_WEEK_HIST";
delete from raw.historical_run_details_from_sf where tableName="DP_PRODUCT_LOCATION_SETTINGS_HIST";
