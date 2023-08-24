-- Databricks notebook source
--*****  Creating table:  "GS_PT_HISTORY" , ***** Creating table: "GS_PT_HISTORY"
use legacy;

CREATE TABLE
    IF NOT EXISTS GS_PT_HISTORY (
        GS_PT_HISTORY_ID BIGINT not null,
        GS_PT_TRAINING_ID BIGINT not null,
        FIELD_NAME STRING not null,
        OLD_VALUE STRING,
        NEW_VALUE STRING,
        UPDATE_BY STRING not null,
        UPDATED_DT TIMESTAMP not null,
        ACTION STRING not null,
        UPDATE_TSTMP TIMESTAMP,
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/gs_pt_history';

--DISTRIBUTE ON (GS_PT_HISTORY_ID)
--*****  Creating table:  "GS_PT_TRAINING" , ***** Creating table: "GS_PT_TRAINING"
use legacy;

CREATE TABLE
    IF NOT EXISTS GS_PT_TRAINING (
        GS_PT_TRAINING_ID BIGINT not null,
        GS_PT_TRAVEL_ID BIGINT,
        HOME_STORE_ID BIGINT not null,
        ASSOCIATE_ID STRING,
        ASSOCIATE_FIRST_NAME STRING not null,
        ASSOCIATE_LAST_NAME STRING not null,
        DM STRING not null,
        TRAINING_STORE_ID BIGINT,
        TRAINING_START_DT TIMESTAMP,
        TRAINING_END_DT TIMESTAMP,
        TRAINING_STATUS INT not null,
        TRAINING_TYPE INT not null,
        CREATED_DT TIMESTAMP not null,
        CREATED_BY STRING,
        MODIFIED_BY STRING,
        MODIFIED_DT TIMESTAMP,
        UPDATE_TSTMP TIMESTAMP,
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/gs_pt_training';

--DISTRIBUTE ON (GS_PT_TRAINING_ID)
--*****  Creating table:  "GS_PT_TRAINING_TYPE" , ***** Creating table: "GS_PT_TRAINING_TYPE"
use legacy;

CREATE TABLE
    IF NOT EXISTS GS_PT_TRAINING_TYPE (
        GS_PT_TRAINING_TYPE_ID INT not null,
        TRAINING_NAME STRING not null,
        CUT_OFF_DAYS INT not null,
        IS_ACTIVE SMALLINT not null,
        UPDATE_TSTMP TIMESTAMP,
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/gs_pt_training_type';

--DISTRIBUTE ON (GS_PT_TRAINING_TYPE_ID)
--*****  Creating table:  "GS_PT_TRAVEL" , ***** Creating table: "GS_PT_TRAVEL"
use legacy;

CREATE TABLE
    IF NOT EXISTS GS_PT_TRAVEL (
        GS_PT_TRAVEL_ID BIGINT not null,
        AIRLINE_DEPART_DT TIMESTAMP,
        AIRLINE_DEPART_TIME STRING,
        AIRLINE_RETURN_DT TIMESTAMP,
        AIRLINE_RETURN_TIME STRING,
        AIRLINE_SEAT_PREFERENCE STRING,
        HOTEL_CHECKIN_DT TIMESTAMP,
        HOTEL_CHECKOUT_DT TIMESTAMP,
        LEGAL_NAME STRING,
        BIRTHDATE TIMESTAMP,
        GENDER SMALLINT,
        PREFERRED_HOTEL STRING,
        PREFERRED_HOTEL_ADDRESS STRING,
        PREFERRED_HOTEL_PHONE STRING,
        PREFERRED_HOTEL_CONTACT STRING,
        PREFERRED_HOTEL_RATE STRING,
        HOTEL_ROOM_TYPE SMALLINT,
        PREFERRED_ROOMMATE STRING,
        MODIFIED_BY STRING,
        UPDATE_TSTMP TIMESTAMP,
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/gs_pt_travel';

--DISTRIBUTE ON (GS_PT_TRAVEL_ID)
--*****  Creating table:  "GS_PT_HISTORY_PRE" , ***** Creating table: "GS_PT_HISTORY_PRE"
use raw;

CREATE TABLE
    IF NOT EXISTS GS_PT_HISTORY_PRE (
        GS_PT_HISTORY_ID BIGINT not null,
        GS_PT_TRAINING_ID BIGINT not null,
        FIELD_NAME STRING not null,
        OLD_VALUE STRING,
        NEW_VALUE STRING,
        UPDATE_BY STRING not null,
        UPDATED_DT TIMESTAMP not null,
        ACTION STRING not null,
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/gs_pt_history_pre';

--DISTRIBUTE ON (GS_PT_HISTORY_ID)
--*****  Creating table:  "GS_PT_TRAINING_PRE" , ***** Creating table: "GS_PT_TRAINING_PRE"
use raw;

CREATE TABLE
    IF NOT EXISTS GS_PT_TRAINING_PRE (
        GS_PT_TRAINING_ID BIGINT not null,
        GS_PT_TRAVEL_ID BIGINT,
        HOME_STORE_ID BIGINT not null,
        ASSOCIATE_ID STRING,
        ASSOCIATE_FIRST_NAME STRING not null,
        ASSOCIATE_LAST_NAME STRING not null,
        DM STRING not null,
        TRAINING_STORE_ID BIGINT,
        TRAINING_START_DT TIMESTAMP,
        TRAINING_END_DT TIMESTAMP,
        TRAINING_STATUS INT not null,
        TRAINING_TYPE INT not null,
        CREATED_DT TIMESTAMP not null,
        CREATED_BY STRING,
        MODIFIED_BY STRING,
        MODIFIED_DT TIMESTAMP,
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/gs_pt_training_pre';

--DISTRIBUTE ON (GS_PT_TRAINING_ID)
--*****  Creating table:  "GS_PT_TRAINING_TYPE_PRE" , ***** Creating table: "GS_PT_TRAINING_TYPE_PRE"
use raw;

CREATE TABLE
    IF NOT EXISTS GS_PT_TRAINING_TYPE_PRE (
        GS_PT_TRAINING_TYPE_ID INT not null,
        TRAINING_NAME STRING not null,
        CUT_OFF_DAYS INT not null,
        IS_ACTIVE SMALLINT not null,
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/gs_pt_training_type_pre';

--DISTRIBUTE ON (GS_PT_TRAINING_TYPE_ID)
--*****  Creating table:  "GS_PT_TRAVEL_PRE" , ***** Creating table: "GS_PT_TRAVEL_PRE"
use raw;

CREATE TABLE
    IF NOT EXISTS GS_PT_TRAVEL_PRE (
        GS_PT_TRAVEL_ID BIGINT not null,
        AIRLINE_DEPART_DT TIMESTAMP,
        AIRLINE_DEPART_TIME STRING,
        AIRLINE_RETURN_DT TIMESTAMP,
        AIRLINE_RETURN_TIME STRING,
        AIRLINE_SEAT_PREFERENCE STRING,
        HOTEL_CHECKIN_DT TIMESTAMP,
        HOTEL_CHECKOUT_DT TIMESTAMP,
        LEGAL_NAME STRING,
        BIRTHDATE TIMESTAMP,
        GENDER SMALLINT,
        PREFERRED_HOTEL STRING,
        PREFERRED_HOTEL_ADDRESS STRING,
        PREFERRED_HOTEL_PHONE STRING,
        PREFERRED_HOTEL_CONTACT STRING,
        PREFERRED_HOTEL_RATE STRING,
        HOTEL_ROOM_TYPE SMALLINT,
        PREFERRED_ROOMMATE STRING,
        MODIFIED_BY STRING,
        LOAD_TSTMP TIMESTAMP
    ) USING delta LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/gs_pt_travel_pre';

--DISTRIBUTE ON (GS_PT_TRAVEL_ID)