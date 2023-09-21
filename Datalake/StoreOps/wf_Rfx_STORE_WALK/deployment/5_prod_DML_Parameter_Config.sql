INSERT INTO raw.parameter_config (parameter_file_name,parameter_section,parameter_key,parameter_value)
VALUES('BA_StoreOPS_Parameter.prm','BA_StoreOPS.WF:wf_Rfx_STORE_WALK.M:m_rfx_store_walk_pre','source_bucket','gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_store_walk/');


INSERT INTO raw.parameter_config (parameter_file_name,parameter_section,parameter_key,parameter_value)
VALUES('BA_StoreOPS_Parameter.prm','BA_StoreOPS.WF:wf_Rfx_STORE_WALK.M:m_rfx_walk_response_pre','source_bucket','gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_walk_response/');

