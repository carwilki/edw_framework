# Databricks notebook source
context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
context = json.loads(context_str)
task_key = context.get('tags', {}).get('taskKey', None)
user_id = context.get('tags', {}).get('user', None)
job_id = context.get('tags', {}).get('jobId', None)

# COMMAND ----------

sql_query = f"""
      INSERT INTO {schema_name}.{log_table}
      (job_id, run_id, task_name, user_id, process, table, status, error_id, updated_time) VALUES
      ({job_id}, {parent_run_id}, '{task_key}', '{user_id}', '{process}', {table}, {status}, {error}, '{load_time}')
    """
    logging.info(sql_query)
    spark.sql(sql_query)
