from pyspark.sql import SparkSession
from logging import getLogger
logger = getLogger()

spark:SparkSession = SparkSession.getActiveSession()

def genMergeUpsertQuery(target_table,source_table,targetColList,primaryKeyString):
  mergeQuery=""" MERGE INTO """+target_table+""" target USING """+source_table+""" source ON """+primaryKeyString
  mergeQuery=mergeQuery+""" WHEN MATCHED AND source.pyspark_data_action=1 THEN UPDATE SET """
  for col in targetColList:
    mergeQuery=mergeQuery+" target."+col+"=source."+col+","
  mergeQuery=mergeQuery.rstrip(',')+""" WHEN NOT MATCHED AND source.pyspark_data_action=0 THEN INSERT ("""
  for col in targetColList:
    mergeQuery=mergeQuery+col+","
  mergeQuery=mergeQuery.rstrip(',')+") VALUES(" 
  for col in targetColList:
    mergeQuery=mergeQuery+"source."+col+"," 
  mergeQuery=mergeQuery.rstrip(',')+")"   
  return mergeQuery



def executeMerge(sourceDataFrame,targetTable,primaryKeyString):
    import deepdiff

    try:
        logger.info("executing executeMerge Function")
        sourceTempView="temp_source_"+targetTable.split('.')[1]
        sourceDataFrame.createOrReplaceTempView(sourceTempView)
        sourceColList=sourceDataFrame.columns
        targetColList=spark.read.table(targetTable).columns  
        

        if "pyspark_data_action" in sourceColList:
            sourceColList.remove('pyspark_data_action')
            listDiff=deepdiff.DeepDiff(sourceColList, targetColList, ignore_string_case=True)
            
            if len(sourceColList)==len(targetColList)  and listDiff=={}:
                upsertQuery=genMergeUpsertQuery(targetTable,sourceTempView,targetColList,primaryKeyString) 
                logger.info("Merge Query ::::::::"+upsertQuery)
                spark.sql(upsertQuery)
                logger.info("Merge Completed Successfully!")

            else:
                raise Exception("Merge not possible due to column mismatch!")
        else:
            raise Exception("Column for Insert/Update 'pyspark_data_action' not available in source!")
    except Exception as e:
        raise e
