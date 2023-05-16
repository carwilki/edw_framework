# Databricks notebook source
# MAGIC %sh
# MAGIC pip install deepdiff

# COMMAND ----------

def genMergeUpsertQuery(target_table,source_table,targetColList):
  mergeQuery=""" MERGE INTO """+target_table+""" target USING """+source_table+""" source ON source.pyspark_data_action=1"""
  mergeQuery=mergeQuery+""" WHEN MATCHED THEN UPDATE SET """
  for col in targetColList:
    mergeQuery=mergeQuery+" target."+col+"=source."+col+","
  mergeQuery=mergeQuery.rstrip(',')+""" WHEN NOT MATCHED THEN INSERT ("""
  for col in targetColList:
    mergeQuery=mergeQuery+col+","
  mergeQuery=mergeQuery.rstrip(',')+") VALUES(" 
  for col in targetColList:
    mergeQuery=mergeQuery+"source."+col+"," 
  mergeQuery=mergeQuery.rstrip(',')+")"   
  return mergeQuery

# COMMAND ----------

def executeMerge(sourceDataFrame,targetTable):
    import deepdiff

    try:
        sourceTempView="temp_source_"+targetTable.split('.')[1]
        sourceDataFrame.createOrReplaceTempView(sourceTempView)
        sourceColList=sourceDataFrame.columns
        targetColList=spark.read.table(targetTable).columns  
        

        if "pyspark_data_action" in sourceColList:
            sourceColList.remove('pyspark_data_action')
            listDiff=deepdiff.DeepDiff(sourceColList, targetColList, ignore_string_case=True)
            
            if len(sourceColList)==len(targetColList)  and listDiff=={}:
                upsertQuery=genMergeUpsertQuery(targetTable,sourceTempView,targetColList) 
                
                spark.sql(upsertQuery)
                print("Merge Completed Successfully!")

            else:
                raise Exception("Merge not possible due to column mismatch!")
        else:
            raise Exception("Column for Insert/Update 'pyspark_data_action' not available in source!")
    except Exception as e:
        raise e

# COMMAND ----------

dbutils.secrets.listScopes()
