INSERT INTO raw.parameter_config (parameter_file_name,parameter_section,parameter_key,parameter_value) VALUES('BA_Inventory_Parameter.prm','BA_Inventory.WF:bs_phys_inv','source_bucket','gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/phys_inv/');

INSERT INTO legacy.PHYS_INV_TYPE(PHYS_INV_TYPE_ID,PHYS_INV_TYPE_DESC,LOAD_DT) VALUES('2','PHYSICAL INV.','2003-07-07T00:00:00.000+0000');

INSERT INTO legacy.PHYS_INV_TYPE(PHYS_INV_TYPE_ID,PHYS_INV_TYPE_DESC,LOAD_DT) VALUES('1','LIVESTOCK CYCT','2003-07-07T00:00:00.000+0000');
