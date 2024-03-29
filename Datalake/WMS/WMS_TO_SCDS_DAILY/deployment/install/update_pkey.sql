update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_ASN_DETAIL_STATUS' where source_table='WM_ASN_DETAIL_STATUS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_CARRIER_ID' where source_table='WM_CARRIER_CODE' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_ELM_ID,WM_CRIT_ID,WM_CRIT_VAL_ID' where source_table='WM_E_ELM_CRIT' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_EQUIPMENT_ID' where source_table='WM_EQUIPMENT' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_EQUIPMENT_INSTANCE_ID' where source_table='WM_EQUIPMENT_INSTANCE' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_ILM_APPOINTMENT_OBJECTS_ID' where source_table='WM_ILM_APPOINTMENT_OBJECTS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_ILM_APPT_STATUS_CD' where source_table='WM_ILM_APPOINTMENT_STATUS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_APPT_TYPE_ID' where source_table='WM_ILM_APPOINTMENT_TYPE' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_APPOINTMENT_ID' where source_table='WM_ILM_APPOINTMENTS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_APPOINTMENT_ID,WM_COMPANY_ID,WM_EQUIPMENT_INSTANCE_ID' where source_table='WM_ILM_APPT_EQUIPMENTS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_ILM_TASK_STATUS' where source_table='WM_ILM_TASK_STATUS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_ACTIVITY_ID' where source_table='WM_ILM_YARD_ACTIVITY' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_ITEM_FACILITY_MAPPING_ID' where source_table='WM_ITEM_FACILITY_SLOTTING' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_ITEM_GROUP_ID' where source_table='WM_ITEM_GROUP_WMS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_LABOR_MSG_CRIT_ID' where source_table='WM_LABOR_MSG_CRIT' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_LABOR_TRAN_DTL_CRIT_ID' where source_table='WM_LABOR_TRAN_DTL_CRIT' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_LOCN_GRP_ID' where source_table='WM_LOCN_GRP' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_OUTPT_ORDER_LINE_ITEM_ID' where source_table='WM_OUTPT_ORDER_LINE_ITEM' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_OUTPT_ORDERS_ID' where source_table='WM_OUTPT_ORDERS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_PICK_LOCN_DTL_ID' where source_table='WM_PICK_LOCN_DTL_SLOTTING' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_PIX_TRAN_ID' where source_table='WM_PIX_TRAN' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_PURCHASE_ORDERS_LINE_STATUS' where source_table='WM_PURCHASE_ORDERS_LINE_STATUS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_SEC_USER_ID' where source_table='WM_SEC_USER' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_SLOT_ITEM_ID' where source_table='WM_SLOT_ITEM' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_SLOT_ITEM_SCORE_ID' where source_table='WM_SLOT_ITEM_SCORE' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_REC_TYPE,WM_CD_TYPE,WM_CD_ID' where source_table='WM_SYS_CODE' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_TRAILER_CONTENTS_ID' where source_table='WM_TRAILER_CONTENTS' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_TRAILER_ID' where source_table='WM_TRAILER_REF' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_TRAILER_TYPE_ID' where source_table='WM_TRAILER_TYPE' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_VISIT_ID' where source_table='WM_TRAILER_VISIT' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_VISIT_DETAIL_ID' where source_table='WM_TRAILER_VISIT_DTL' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_VEND_PERF_TRAN_ID' where source_table='WM_VEND_PERF_TRAN' and  table_group='NZ_Migration';
update work.rocky_ingestion_metadata set primary_key='LOCATION_ID,WM_WAVE_PARM_ID' where source_table='WM_WAVE_PARM' and  table_group='NZ_Migration';
