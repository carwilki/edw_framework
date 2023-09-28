use legacy;

CREATE TABLE 
  inventory_event_str_sku (
    LOCATION_ID INT NOT NULL,
    STO_TYPE_ID INT NOT NULL,
    PRODUCT_ID INT NOT NULL,
    QUANTITY DECIMAL(13,3)
    ) USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/labor_mgmt/inventory_event_str_sku';
 
 use legacy;

 CREATE TABLE 
  sto_type_lu (
    STO_TYPE_ID INT NOT NULL,
    STO_TYPE STRING NOT NULL,
    STR_BEG_DATE TIMESTAMP,
    STR_END_DATE TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/labor_mgmt/sto_type_lu';

  use legacy;

  CREATE TABLE 
    holiday_sto (
      PO_NBR BIGINT NOT NULL,
      VENDOR_ID BIGINT,
      LOCATION_ID INT,
      PURCH_GROUP_ID INT NOT NULL,
      STO_TYPE_ID INT NOT NULL,
      DELIVERY_DATE TIMESTAMP,
      EXE_SENT_DATE TIMESTAMP,
      TMS_SENT_DATE TIMESTAMP,
      TMS_PROCESSED TINYINT NOT NULL
      ) USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/labor_mgmt/holiday_sto';
  
use raw;

CREATE TABLE 
  ztb_event_ctrl_pre (
    MANDT STRING,
    STO_TYPE STRING,
    STR_BEG_DATE TIMESTAMP,
    STR_END_DATE TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/labor_mgmt/ztb_event_ctrl_pre';

use raw;

CREATE TABLE 
  ztb_event_hold_pre (
    MANDT STRING NOT NULL,
    STORE_NBR INT NOT NULL,
    STO_TYPE STRING NOT NULL,
    SKU_NBR INT NOT NULL,
    QUANTITY DECIMAL(13,3) NOT NULL
    ) USING delta LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/labor_mgmt/ztb_event_hold_pre';

use raw;

CREATE TABLE 
  ztb_holiday_sto_pre (
    CLIENT STRING NOT NULL,
    PO_NBR BIGINT NOT NULL,
    VENDOR_ID BIGINT,
    STORE_NBR INT,
    PURCH_GROUP_ID INT NOT NULL,
    STO_TYPE STRING,
    DELIVERY_DATE TIMESTAMP,
    EXE_SENT_DATE TIMESTAMP,
    TMS_SENT_DATE TIMESTAMP,
    TMS_PROCESSED TINYINT NOT NULL
    ) USING delta LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/labor_mgmt/ztb_holiday_sto_pre';




  