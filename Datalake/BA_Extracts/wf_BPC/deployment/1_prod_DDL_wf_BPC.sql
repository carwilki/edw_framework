use legacy;
CREATE TABLE LOCATION_TYPE 
(
LOCATION_TYPE_ID TINYINT not null
 
, LOCATION_TYPE_DESC STRING not null
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Extracts/location_type';
 
 
 
 
USE LEGACY;
 
CREATE VIEW LOCATION_AREA_VIEW
AS 
SELECT W.WEEK_DT
  ,LA.LOCATION_ID
  ,LA.AREA_ID
  ,LA.SQ_FT_AMT
FROM LEGACY.LOCATION_AREA LA
  ,LEGACY.WEEKS W
WHERE (
    (
      (W.WEEK_DT >= LA.LOC_AREA_EFF_DT)
      AND (W.WEEK_DT <= LA.LOC_AREA_END_DT)
      )
    AND (
      (W.WEEK_DT >= date_sub(now(), 371))
      AND (W.WEEK_DT <= date_add(now(), 28))
      )
    );


USE LEGACY;
insert into LOCATION_TYPE select * from qa_legacy.LOCATION_TYPE;