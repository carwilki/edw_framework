from uuid import uuid4
from pyspark.sql import SparkSession, DataFrame
from logging import getLogger
from Datalake.utils.genericUtilities import getEnvPrefix, getSfCredentials
from Datalake.utils.SF_Merge_Utils import SnowflakeWriter, getAppendQuery
import deepdiff

from Datalake.utils.pk.pk import DuplicateChecker


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


def executeMerge(sourceDataFrame: str, targetTable: str, primaryKeyString: str):
    logger = getLogger()

    try:
        logger.info("executing executeMerge Function")
        sourceTempView = "temp_source_" + targetTable.replace(".", "_")
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


def executeMergeByPrimaryKey(
    sourceDataFrame: DataFrame, targetTable: str, primaryKeys: list[str]
):
    keystatments: list[str] = []
    for key in primaryKeys:
        keystatments.append("target." + key + "=source." + key)
    mergecondition = " AND ".join(keystatments)
    tempTarget = (
        f"temp_target_{targetTable.replace('.','_')}_{str(uuid4()).replace('-','')}"
    )
    createTempTable = f"CREATE TEMP VIEW {tempTarget} AS SELECT * FROM {targetTable}"
    spark.sql(createTempTable).collect()
    executeMerge(sourceDataFrame, tempTarget, mergecondition)
    DuplicateChecker.check_for_duplicate_primary_keys(tempTarget, targetTable)
    df = spark.sql(f"select * from {tempTarget}")
    df.write.mode("overwrite").saveAsTable(targetTable)


def MergeToSF(env, deltaTable, primaryKeys, conditionCols):
    print("Merge_To_SF function")
    import json
    from logging import getLogger

    logger = getLogger()
    sfOptions = getSfCredentials(env)
    append_query = getAppendQuery(env, deltaTable, conditionCols)
    schemaForDeltaTable = getEnvPrefix(env) + "refine"

    mergeDatasetSql = (
        f"""select * from `{schemaForDeltaTable}`.`{deltaTable}` where {append_query}"""
    )

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


def mergeToSFv2(env, deltaTable, primaryKeys, conditionCols):
    import datetime as dt

    print("Merge_To_SF function")
    import json
    from logging import getLogger

    from Datalake.utils.genericUtilities import getEnvPrefix, getSfCredentials
    from Datalake.utils.SF_Merge_Utils_v2 import SnowflakeWriter, getAppendQuery

    logger = getLogger()
    sfOptions = getSfCredentials(env)
    append_query = getAppendQuery(env, deltaTable, conditionCols)
    schemaForDeltaTable = getEnvPrefix(env) + "refine"
    conditionCols: list[str] = list(filter(None, conditionCols))

    if len(conditionCols) == 0:
        mergeDatasetSql = f"""select * from `{schemaForDeltaTable}`.`{deltaTable}`"""
    else:
        mergeDatasetSql = f"""select * from `{schemaForDeltaTable}`.`{deltaTable}` where {append_query}"""

    print(mergeDatasetSql)

    df_table = spark.sql(mergeDatasetSql)

    row_count = df_table.count()
    SFTable = f"{deltaTable}"

    if row_count == 0:
        logger.info("No new records to insert or update into Snowflake")
    else:
        SnowflakeWriter(
            sfOptions,
            SFTable,
            json.loads(primaryKeys),
        ).push_data(df_table, write_mode="merge")


def mergeToSFLegacy(env, deltaTable, primaryKeys, conditionCols):
    import datetime as dt

    print("Merge_To_SF function")
    import json
    from logging import getLogger

    from Datalake.utils.genericUtilities import getEnvPrefix, getSfCredentials
    from Datalake.utils.SF_Merge_Utils_v2 import SnowflakeWriter, getAppendQuery

    logger = getLogger()
    sfOptions = getSfCredentials(env)
    append_query = getAppendQuery(env, deltaTable, conditionCols)
    schemaForDeltaTable = getEnvPrefix(env) + "legacy"
    conditionCols: list[str] = list(filter(None, conditionCols))

    if len(conditionCols) == 0:
        mergeDatasetSql = f"""select * from `{schemaForDeltaTable}`.`{deltaTable}`"""
    else:
        mergeDatasetSql = f"""select * from `{schemaForDeltaTable}`.`{deltaTable}` where {append_query}"""

    print(mergeDatasetSql)

    df_table = spark.sql(mergeDatasetSql)

    row_count = df_table.count()
    SFTable = f"{deltaTable}_lgcy"

    if row_count == 0:
        logger.info("No new records to insert or update into Snowflake")
    else:
        SnowflakeWriter(
            sfOptions,
            SFTable,
            json.loads(primaryKeys),
        ).push_data(df_table, write_mode="merge")


def mergeToSFPII(
    env,
    deltaTableSchema,
    deltaTable,
    primaryKeys,
    conditionCols,
    refineOrLegacy,
    mode,
    SFSchema=None,
):
    import datetime as dt
    import json
    from logging import getLogger

    from Datalake.utils.genericUtilities import getEnvPrefix, getSfCredentials
    from Datalake.utils.SF_Merge_Utils_v2 import SnowflakeWriter, getAppendQuery

    logger = getLogger()
    sfOptions = getSfCredentials(env)

    if SFSchema is not None:
        sfOptions[
            "sfSchema"
        ] = SFSchema  # Overriding sfOptions schema if provided as input parameters

    ### Adding _lgcy suffix to snowflake table based on whether its legacy or refine
    if refineOrLegacy.lower() == "refine":
        deltaTableName = f"refine_{deltaTable}"
        SFTable = deltaTable
    elif refineOrLegacy.lower() == "legacy":
        deltaTableName = f"legacy_{deltaTable}"
        SFTable = f"{deltaTable}_lgcy"

    schemaForDeltaTable = getEnvPrefix(env) + deltaTableSchema

    if mode.lower() == "merge":
        logger.info("Started merging data to Snowflake tables for table - ", deltaTable)
        append_query = getAppendQuery(env, deltaTable, conditionCols)

        mergeDatasetSql = f"""select * from `{schemaForDeltaTable}`.`{deltaTableName}` where {append_query}"""

        print(mergeDatasetSql)

        df_table = spark.sql(mergeDatasetSql)

        row_count = df_table.count()

        if row_count == 0:
            logger.info("No new records to insert or update into Snowflake")
        else:
            SnowflakeWriter(
                sfOptions,
                SFTable,
                json.loads(primaryKeys),
            ).push_data(df_table, write_mode="merge")
            logger.info(
                "Completed merging data to Snowflake tables for table - ", deltaTable
            )

    elif mode.lower() == "overwrite":
        logger.info(
            "Started Overwriting data to Snowflake tables for table - ", deltaTable
        )

        df_table = spark.table(f"{schemaForDeltaTable}.{deltaTableName}")
        if refineOrLegacy.lower() == "legacy":
            df_table = df_table.withColumn(
                "SNF_LOAD_TSTMP", current_timestamp()
            ).withColumn("SNF_UPDATE_TSTMP", current_timestamp())

        SnowflakeWriter(sfOptions, SFTable).push_data(df_table, write_mode="full")
        logger.info(
            "Completed Overwriting data to Snowflake tables for table - ", deltaTable
        )

    elif mode.lower() == "append":
        logger.info(
            "Started appending data to Snowflake tables for table - ", deltaTable
        )

        df_table = spark.table(f"{schemaForDeltaTable}.{deltaTableName}")
        if refineOrLegacy.lower() == "legacy":
            df_table = df_table.withColumn(
                "SNF_LOAD_TSTMP", current_timestamp()
            ).withColumn("SNF_UPDATE_TSTMP", current_timestamp())

        SnowflakeWriter(sfOptions, SFTable).push_data(df_table, write_mode="append")
        logger.info(
            "Completed appending data to Snowflake tables for table - ", deltaTable
        )


def IngestFromSFHistoricalData(env, deltaTable):
    print("Ingest_historical_data_from_Snowflake")
    import json
    from logging import getLogger

    from Datalake.utils.genericUtilities import getEnvPrefix, getSfCredentials
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
