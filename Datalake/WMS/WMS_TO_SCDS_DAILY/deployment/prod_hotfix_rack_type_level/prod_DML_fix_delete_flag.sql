-- SHOULD ONLY UPDATE 4 RECORDS
UPDATE REFINE.wm_rack_type_level
SET DELETE_FLAG = 0 ,
    UPDATE_TSTMP = CURRENT_TIMESTAMP
WHERE DELETE_FLAG = 1;