import json
from logging import getLogger,INFO
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark:SparkSession = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

#Function to Log the Success/Failure to log_run_details table
# #Usage   - logPrevRunDt('test','test','Completed','N/A',"devrefine.log_run_details")  
def logPrevRunDt(process,table_name,status,error,logTableName):
    
    from datetime import datetime as dt
    # Getting current date and time
    now = dt.now()
    print("Without formatting", now)

    s = now.strftime("%Y-%m-%d %H:%M:%S")
    s = str(s)

    context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    context = json.loads(context_str)
    task_name = context.get('tags', {}).get('taskKey', None)
    job_id = context.get('tags', {}).get('jobId', None)
    run_id_obj = context.get('currentRunId', {})
    run_id = run_id_obj.get('id', None) if run_id_obj else None

    if task_name  or job_id or run_id is None:
        task_name='null'
        job_id=run_id=1

    
    if status.lower()=='failed':
        
        sql_query = f"""
        INSERT INTO {logTableName}
        (job_id, run_id, task_name,  process, table_name, status, error, prev_run_date) VALUES
        ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table_name}', '{status}', '{error}', null)
        """
    else:
        sql_query = f"""
        INSERT INTO {logTableName}
        (job_id, run_id, task_name,  process, table_name, status, error, prev_run_date) VALUES
        ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table_name}', '{status}', '{error}', '{s}')
        """

    print('Logging the status')    
    print(sql_query)
    spark.sql(sql_query)
    print('Logging Completed')