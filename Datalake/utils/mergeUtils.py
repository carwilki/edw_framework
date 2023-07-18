from pyspark.sql import SparkSession
from Datalake.utils.genericUtilities import getSfCredentials
from logging import getLogger

logger = getLogger()

spark: SparkSession = SparkSession.getActiveSession()


def genMergeUpsertQuery(target_table, source_table, targetColList, primaryKeyString):
    mergeQuery = (
        """ MERGE INTO """
        + target_table
        + """ target USING """
        + source_table
        + """ source ON """
        + primaryKeyString
    )
    mergeQuery = (
        mergeQuery
        + """ WHEN MATCHED AND source.pyspark_data_action=1 THEN UPDATE SET """
    )
    for col in targetColList:
        mergeQuery = mergeQuery + " target." + col + "=source." + col + ","
    mergeQuery = (
        mergeQuery.rstrip(",")
        + """ WHEN NOT MATCHED AND source.pyspark_data_action=0 THEN INSERT ("""
    )
    for col in targetColList:
        mergeQuery = mergeQuery + col + ","
    mergeQuery = mergeQuery.rstrip(",") + ") VALUES("
    for col in targetColList:
        mergeQuery = mergeQuery + "source." + col + ","
    mergeQuery = mergeQuery.rstrip(",") + ")"
    return mergeQuery


def executeMerge(sourceDataFrame, targetTable, primaryKeyString):
    import deepdiff
    from logging import getLogger, INFO

    logger = getLogger()

    try:
        logger.info("executing executeMerge Function")
        sourceTempView = "temp_source_" + targetTable.split(".")[1]
        sourceDataFrame.createOrReplaceTempView(sourceTempView)
        sourceColList = sourceDataFrame.columns
        targetColList = spark.read.table(targetTable).columns

        if "pyspark_data_action" in sourceColList:
            sourceColList.remove("pyspark_data_action")
            listDiff = deepdiff.DeepDiff(
                sourceColList, targetColList, ignore_string_case=True
            )

            if len(sourceColList) == len(targetColList) and listDiff == {}:
                upsertQuery = genMergeUpsertQuery(
                    targetTable, sourceTempView, targetColList, primaryKeyString
                )
                logger.info("Merge Query ::::::::" + upsertQuery)
                spark.sql(upsertQuery)
                logger.info("Merge Completed Successfully!")

            else:
                raise Exception("Merge not possible due to column mismatch!")
        else:
            raise Exception(
                "Column for Insert/Update 'pyspark_data_action' not available in source!"
            )
    except Exception as e:
        raise e


def MergeToSF(env, deltaTable, primaryKeys, conditionCols):
    import datetime as dt
    
    print("Merge_To_SF function")
    from Datalake.utils.genericUtilities import getSfCredentials
    from logging import getLogger
    import json
    from Datalake.utils.genericUtilities import getEnvPrefix
    from Datalake.utils.SF_Merge_Utils import SnowflakeWriter, getAppendQuery

    logger = getLogger()
    sfOptions = getSfCredentials(env)
    append_query = getAppendQuery(env, deltaTable, conditionCols)
    schemaForDeltaTable = getEnvPrefix(env) + "refine"
    
    conditionColList=json.loads(conditionCols)
    
    if len(conditionColList)==1 and conditionColList[0]=="":
        refineDF=spark.sql(f"""show columns in `{schemaForDeltaTable}`.`{deltaTable}`""")
        refineColList=[row.col_name.upper() for row in refineDF.collect()]
        if "LOAD_TSTMP" in refineColList:
            dateValue = dt.datetime.today() - dt.timedelta(days=2)
            mergeDatasetSql = (
        f"""select * from `{schemaForDeltaTable}`.`{deltaTable}` where LOAD_TSTMP > {dateValue}""")
        else:
            mergeDatasetSql = (
        f"""select * from `{schemaForDeltaTable}`.`{deltaTable}` """)

    else:            
        mergeDatasetSql = (
        f"""select * from `{schemaForDeltaTable}`.`{deltaTable}` where {append_query}""" )
    
    print(mergeDatasetSql)

    df_table = spark.sql(mergeDatasetSql)

    row_count = df_table.count()
    SFTable = f"{deltaTable}"

    if row_count == 0:
        logger.info("No new records to insert or update into Snowflake")
    else:
        SnowflakeWriter(
            env,
            sfOptions["sfDatabase"],
            sfOptions["sfSchema"],
            SFTable,
            json.loads(primaryKeys),
        ).push_data(df_table, write_mode="merge")


def IngestFromSFHistoricalData(env, deltaTable):
    print("Ingest_historical_data_from_Snowflake")
    from Datalake.utils.genericUtilities import getSfCredentials
    from logging import getLogger
    import json
    from Datalake.utils.genericUtilities import getEnvPrefix
    from Datalake.utils.SF_Merge_Utils import SnowflakeWriter, getAppendQuery

    logger = getLogger()
    sfOptions = getSfCredentials(env)

    schemaForDeltaTable = getEnvPrefix(env) + "refine"

    ingestDatasetSql = f"""insert overwrite table `{schemaForDeltaTable}`.`{deltaTable}` select * from `{sfOptions.sfDatabase}`.`{deltaTable}`"""
    print(mergeDatasetSql)

    df_table = spark.sql(mergeDatasetSql)

    row_count = df_table.count()
    SFTable = f"{deltaTable}"

    if row_count == 0:
        logger.info("No new records to insert or update into Snowflake")
    else:
        SnowflakeWriter(
            env,
            sfOptions["sfDatabase"],
            sfOptions["sfSchema"],
            SFTable,
            json.loads(primaryKeys),
        ).push_data(df_table, write_mode="merge")
