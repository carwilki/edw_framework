-- Databricks notebook source
-- DBTITLE 1,Count checks between qa and prod tables
SELECT 'qa_legacy.svc_service_suspension_log' AS TABLE_NAME, COUNT(*) AS CNT
  FROM qa_legacy.svc_service_suspension_log
UNION ALL
SELECT 'legacy.svc_service_suspension_log' AS TABLE_NAME, COUNT(*) AS CNT
  FROM legacy.svc_service_suspension_log;

-- COMMAND ----------

SELECT 'qa_legacy.SVC_SUSPENSION_SUBMITTER_TITLE' AS TABLE_NAME, COUNT(*) AS CNT
  FROM qa_legacy.SVC_SUSPENSION_SUBMITTER_TITLE
UNION ALL
SELECT 'legacy.SVC_SUSPENSION_SUBMITTER_TITLE' AS TABLE_NAME, COUNT(*) AS CNT
  FROM legacy.SVC_SUSPENSION_SUBMITTER_TITLE;

-- COMMAND ----------

SELECT 'qa_legacy.SVC_SERVICE_AREA' AS TABLE_NAME, COUNT(*) AS CNT
  FROM qa_legacy.SVC_SERVICE_AREA
UNION ALL
SELECT 'legacy.SVC_SERVICE_AREA' AS TABLE_NAME, COUNT(*) AS CNT
  FROM legacy.SVC_SERVICE_AREA;

-- COMMAND ----------

SELECT 'qa_legacy.SVC_SERVICE_SUSPENSION_REASON' AS TABLE_NAME, COUNT(*) AS CNT
  FROM qa_legacy.SVC_SERVICE_SUSPENSION_REASON
UNION ALL
SELECT 'legacy.SVC_SERVICE_SUSPENSION_REASON' AS TABLE_NAME, COUNT(*) AS CNT
  FROM legacy.SVC_SERVICE_SUSPENSION_REASON;

-- COMMAND ----------

select * from legacy.svc_service_suspension_log;

-- COMMAND ----------

select * from legacy.SVC_SUSPENSION_SUBMITTER_TITLE;

-- COMMAND ----------

select * from legacy.SVC_SERVICE_AREA;

-- COMMAND ----------

select * from legacy.SVC_SERVICE_SUSPENSION_REASON;
