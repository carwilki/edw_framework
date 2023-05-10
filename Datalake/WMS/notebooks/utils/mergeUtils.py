# Databricks notebook source
# MAGIC %sh
# MAGIC pip install deepdiff

# COMMAND ----------

def genMergeUpsertQuery(target_table,source_table,primaryKeyList):
            mergeQuery=""" MERGE INTO """+target_table+""" target USING """+source_table+""" source ON """
            for pKey in primaryKeyList:
                mergeQuery=mergeQuery+" target."+pKey+"= source."+pKey+" AND"
            mergeQuery=mergeQuery.rstrip('AND')
            mergeQuery=mergeQuery+""" WHEN MATCHED THEN UPDATE SET *"""
            
            mergeQuery=mergeQuery+""" WHEN NOT MATCHED THEN INSERT *"""
              
            return mergeQuery

# COMMAND ----------

def executeMerge(sourceDataFrame,targetTable,primaryKeyList):
    import deepdiff

    #Source Table :Shortcut_to_WM_E_CONSOL_PERF_SMRY
    #Shortcut_to_WM_E_CONSOL_PERF_SMRY.createOrReplaceTempView(temp_wm_e_consol-perf_smry)
    try:
        sourceDataFrame.createOrReplaceTempView("temp_"+sourceDataFrame)
        sourceColList=sourceDataFrame.columns
        targetColList=spark.read.table(targetTable).columns  

        listDiff=deepdiff.DeepDiff(sourceColList, targetColList, ignore_string_case=True)
        if len(sourceColList)==len(targetColList)  and listDiff=={}:
            upsertQuery=genMergeUpsertQuery(targetTable,"temp_"+sourceDataFrame,primaryKeyList) 
            print(upsertQuery)
            #spark.sql(upsertQuery) # TO UNCOMMENT AFTER TEST
            print("Merge Successfull!")

        else:
            raise Exception("Merge not possible due to column mismatch!")

    except Exception as e:
        raise e
