# Databricks notebook source
INSERT INTO work.pii_dynamic_view_control (
secured_database, secured_table_name, view_database, view_name,
view_created_flg, load_user_name, load_dt_tm, update_dt_tm
)
VALUES (
"cust_sensitive", "legacy_src_services_reservation", "legacy", "src_services_reservation",
False, "rjalan", current_timestamp(), current_timestamp()
);

# COMMAND ----------

INSERT INTO work.pii_dynamic_view_control (
secured_database, secured_table_name, view_database, view_name,
view_created_flg, load_user_name, load_dt_tm, update_dt_tm
)
VALUES (
"cust_sensitive", "raw_src_services_reservation_pre", "raw", "src_services_reservation_pre",
False, "rjalan", current_timestamp(), current_timestamp()
);

# COMMAND ----------

insert into pii_metadata.pii_metadata_store select * from pii_metadata.pii_metadata_store_qa where table_name = 'raw_src_services_reservation_pre'
