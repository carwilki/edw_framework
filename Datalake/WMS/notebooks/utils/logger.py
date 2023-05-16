# Databricks notebook source
import json

# COMMAND ----------

#Function to Log the Success/Failure to log_run_details table
# #Usage   - logPrevRunDt('test','test','Completed','N/A',"dev_refine.log_run_details")  
def logPrevRunDt(process,table_name,status,error,logTableName):

    context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    context = json.loads(context_str)
    task_name = context.get('tags', {}).get('taskKey', None)
    job_id = context.get('tags', {}).get('jobId', None)
    run_id_obj = context.get('currentRunId', {})
    run_id = run_id_obj.get('id', None) if run_id_obj else None

    if task_name  or job_id or run_id is None:
        task_name='null'
        job_id=run_id=1

    sql_query = f"""
        INSERT INTO {logTableName}
        (job_id, run_id, task_name,  process, table_name, status, error, prev_run_date) VALUES
        ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table_name}', '{status}', '{error}', current_timestamp())
        """
        
    print('Logging the status')    
    print(sql_query)
    spark.sql(sql_query)
    print('Logging Completed')
