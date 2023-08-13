class DeltaLakeWriter:
    def __init__(self, sfOptions, table, delta_schema):
        from pyspark.sql import SparkSession
        from Datalake.utils.genericUtilities import getEnvPrefix

        self.spark: SparkSession = SparkSession.getActiveSession()
        print("initiating DeltaLake Writer class")
        from pyspark.dbutils import DBUtils

        self.dbutils = DBUtils(self.spark)
        self.table = table
        self.env = sfOptions["env"]
        self.sfOptions = sfOptions
        self.delta_schema = delta_schema

        self.raw = getEnvPrefix(self.env) + "raw"

    def genPrevRunDt(self, refine_table_name, refine, raw):
        from logging import getLogger, INFO
        from datetime import datetime

        print("get Prev_run date")
        logger = getLogger()

        prev_run_dt = self.spark.sql(
            f"""select max(run_date)
        from {raw}.historical_run_details_from_sf
        where lower(table_name)=lower('{refine_table_name}') and lower(status)= lower('Succeeded')"""
        ).collect()[0][0]
        logger.info("Extracted prev_run_dt from historical_run_details_from_sf table")

        if prev_run_dt is not None:
            prev_run_dt = datetime.strptime(str(prev_run_dt), "%Y-%m-%d %H:%M:%S")
            prev_run_dt = prev_run_dt.strftime("%Y-%m-%d")

        return prev_run_dt

    def getAppendQuery(self, env, deltaTable, conditionCols):
        print("get Append query")
        from pyspark.sql import SparkSession
        from Datalake.utils.genericUtilities import getEnvPrefix
        import json
        from datetime import datetime, timedelta

        raw = getEnvPrefix(env) + "raw"

        prev_run_dt = self.spark.sql(
            f"""select max(run_date)  from {raw}.historical_run_details_from_sf where lower(table_name)=lower('{deltaTable}') and lower(status)= lower('Succeeded')"""
        ).collect()[0][0]
        # prev_run_dt = "2023-01-01 00:36:24"
        prev_run_dt = datetime.strptime(str(prev_run_dt), "%Y-%m-%d %H:%M:%S")
        prev_run_dt = prev_run_dt - timedelta(days=15)
        prev_run_dt = prev_run_dt.strftime("%Y-%m-%d")
        append_query = ""
        ## for tesr ######

        for i in conditionCols:
            if conditionCols.index(i) == 0:
                append_query = (
                    append_query
                    + f"""DATE_TRUNC('DAY',cast({i} as date)) >= '{prev_run_dt}'"""
                )
            else:
                append_query = (
                    append_query
                    + f""" or DATE_TRUNC('DAY',cast({i} as date)) >= '{prev_run_dt}'"""
                )

        return append_query

    def logRun(
        self, process, table, sf_row_count, delta_row_count, status, error, logTableName
    ):
        from logging import getLogger, INFO
        import json

        logger = getLogger()
        from datetime import datetime as dt, timedelta

        # Getting current date and time
        now = dt.now()
        # now = dt.now() - timedelta(days=211)

        s = now.strftime("%Y-%m-%d %H:%M:%S")
        s = str(s)

        context_str = (
            self.dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .toJson()
        )
        context = json.loads(context_str)
        task_name = context.get("tags", {}).get("taskKey", None)
        job_id = context.get("tags", {}).get("jobId", None)
        run_id_obj = context.get("currentRunId", {})
        run_id = run_id_obj.get("id", None) if run_id_obj else None

        if task_name or job_id or run_id is None:
            task_name = "null"
            job_id = run_id = 1

        if status.lower() == "failed":
            logger.info(
                "Inserting failed run details into historical_run_details_from_sf table"
            )
            sql_query = f"""
            INSERT INTO {logTableName}
            (job_id, run_id, task_name,  process, table_name,sf_rowCount, delta_rowCount,  status, error, run_date) VALUES
            ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table}', '{sf_row_count}', '{delta_row_count}', '{status}', '{error}', '{s}')
         """
        else:
            logger.info(
                "Inserting success run details into historical_run_details_from_sf table"
            )
            sql_query = f"""
            INSERT INTO {logTableName}
            (job_id, run_id, task_name,  process, table_name,sf_rowCount, delta_rowCount, status, error, run_date) VALUES
            ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table}', '{sf_row_count}', '{delta_row_count}', '{status}', '{error}', '{s}')
            """

        logger.info("Logging the status")
        logger.info(sql_query)
        self.spark.sql(sql_query)
        logger.info("Logging Completed")

    def generateClause(self, column_list, clause_type):
        clause_type = clause_type.lower()
        clause = ""
        for k in column_list:
            if clause == "":
                if clause_type == "update":
                    clause = "target." + k + "=source." + k
                elif clause_type == "insert":
                    clause = "source." + k

            else:
                if clause_type == "update":
                    clause = clause + " , " + "target." + k + " = source." + k
                elif clause_type == "insert":
                    clause = clause + ",source." + k
        return clause

    def generateMergequery(self, cols, sfTable, schemaForDeltaTable):
        from logging import getLogger, INFO

        logger = getLogger()
        sfTable = sfTable + "_vw"
        pkey_df = self.spark.table(f"{self.raw}.sf2delta_pKey_tstcols")

        try:
            pKey_columns = (
                pkey_df.select("pKeys")
                .where("lower(tableName)='" + self.table.lower() + "'")
                .first()[0]
            )
        except Exception as e:
            logger.info(
                f"{self.table} not added to dev_raw.sf2delta_pKey_tstcols table"
            )
            raise e

        pKey_columns_list = pKey_columns.split("|")
        primaryKeyString = ""
        for i in range(len(pKey_columns_list)):
            if pKey_columns_list[i] == pKey_columns_list[-1]:
                primaryKeyString = (
                    primaryKeyString
                    + f"""source.{pKey_columns_list[i]} = target.{pKey_columns_list[i]} """
                )
            else:
                primaryKeyString = (
                    primaryKeyString
                    + f"""source.{pKey_columns_list[i]} = target.{pKey_columns_list[i]} AND """
                )

        return f"""merge into {schemaForDeltaTable}.{self.table} as target using {sfTable} as source on  {primaryKeyString}
                   when matched then update set  {self.generateClause(cols, "update")}
                   when not matched then insert ({','.join(cols)}) VALUES ({self.generateClause(cols, "insert")})"""

    def pull_data(self):
        from logging import getLogger, INFO
        from Datalake.utils.genericUtilities import getEnvPrefix, sfReader
        from pyspark.sql.types import (
            DecimalType,
            ShortType,
            IntegerType,
            LongType,
            ByteType,
        )
        from pyspark.sql.functions import col

        logger = getLogger()

        # delta_schema = "refine"
        schemaForDeltaTable = getEnvPrefix(self.env) + self.delta_schema
        # delta_table = f"""{schemaForDeltaTable}.{self.table}"""
        refine_table_name = self.table
        # refine = schemaForDeltaTable
        # raw = self.raw

        try:
            query = ""
            logger.info(f"Getting data for table {0}".format(self.table))
            # print(self.sfOptions, self.table)
            sfTable = self.table.lower() + "_nz"
            print("sftable", {sfTable})
            prev_run_dt = self.genPrevRunDt(
                refine_table_name, schemaForDeltaTable, self.raw
            )
            metadata_df = self.spark.table(f"{self.raw}.sf2delta_pKey_tstcols")
            tstmp_cols1 = (
                metadata_df.select("tstmp_cols1")
                .where("lower(tableName)='" + refine_table_name.lower() + "'")
                .first()[0]
            )

            if prev_run_dt is None or tstmp_cols1 is None:
                write_mode = "full"
                query = f"""select * from {sfTable}"""
            else:
                logger = getLogger()

                try:
                    columns = (
                        metadata_df.select("tstmp_cols1")
                        .where("lower(tableName)='" + refine_table_name.lower() + "'")
                        .first()[0]
                    )
                except Exception as e:
                    logger.info(
                        f"{refine_table_name} not added to dev_raw.sf2delta_pKey_tstcols table"
                    )
                    raise e
                conditionColList = columns.split("|")
                # print(conditionColList)
                append_query = self.getAppendQuery(
                    self.env, refine_table_name, conditionColList
                )
                write_mode = "merge"
                print(append_query)
                query = f"""select * from {sfTable} where {append_query}"""
                print(query)

            print(query)
            df_sf = sfReader(self.sfOptions, query)
            for field in df_sf.schema.fields:
                if isinstance(field.dataType, DecimalType):
                    if field.dataType.scale == 0:
                        if 0 < field.dataType.precision <= 2:
                            df_sf = df_sf.withColumn(
                                field.name, col(field.name).cast(ByteType())
                            )
                        elif 3 <= field.dataType.precision <= 4:
                            df_sf = df_sf.withColumn(
                                field.name, col(field.name).cast(ShortType())
                            )
                        elif (
                            5 <= field.dataType.precision <= 9
                            or field.dataType.precision == 38
                        ):
                            df_sf = df_sf.withColumn(
                                field.name, col(field.name).cast(IntegerType())
                            )
                        elif 10 <= field.dataType.precision <= 18:
                            df_sf = df_sf.withColumn(
                                field.name, col(field.name).cast(LongType())
                            )
            # print("SNF_LOAD_TSTMP", df_sf.select(max("SNF_LOAD_TSTMP")))
            df_sf = df_sf.drop(col("SNF_UPDATE_TSTMP"))
            df_sf = df_sf.drop(col("SNF_LOAD_TSTMP"))

            print("Count of incremental records", df_sf.count())

            if write_mode.lower() == "merge":
                df_sf.createOrReplaceTempView(sfTable + "_vw")
                merge_query = self.generateMergequery(
                    df_sf.columns, sfTable, schemaForDeltaTable
                )
                print("running upsert ", merge_query)
                try:
                    logger.info("Executing merge query")
                    self.spark.sql(merge_query)
                    print("Merge Completed Successfully!")
                    query_post_merge = f"""select * from {sfTable}"""
                    sf_df_post_merge = sfReader(self.sfOptions, query_post_merge)
                    sf_row_count_post_merge = sf_df_post_merge.count()
                    df_delta = self.spark.read.table(
                        f"{schemaForDeltaTable}.{self.table}"
                    )
                    delta_row_count_post_merge = df_delta.count()
                    if sf_row_count_post_merge == delta_row_count_post_merge:
                        self.logRun(
                            process="Delta Writer -" + self.table,
                            table=self.table,
                            sf_row_count=sf_row_count_post_merge,
                            delta_row_count=delta_row_count_post_merge,
                            status="Merge Successful",
                            error="No error",
                            logTableName=f"{self.raw}.historical_run_details_from_sf",
                        )
                    else:
                        self.logRun(
                            process="Delta Writer -" + self.table,
                            table=self.table,
                            sf_row_count=sf_row_count_post_merge,
                            delta_row_count=delta_row_count_post_merge,
                            status="Merge Successful but row count mismtach between source/target",
                            error="Merge Successful but row count mismtach between source/target",
                            logTableName=f"{self.raw}.historical_run_details_from_sf",
                        )

                except Exception as e:
                    logger.info("Merge cannot complete!")
                    self.logRun(
                        process="Delta Writer -" + self.table,
                        table=self.table,
                        sf_row_count=sf_row_count_post_merge,
                        delta_row_count=delta_row_count_post_merge,
                        status="Merge failed!",
                        error="Merge cannot complete!",
                        logTableName=f"{self.raw}.historical_run_details_from_sf",
                    )
                    raise e
            elif write_mode.lower() == "full":
                self.sf2delta_fullLoad(df_sf, schemaForDeltaTable)
        except Exception as e:
            logger.info(f"{refine_table_name} something went wrong")
            raise e

    def sf2delta_fullLoad(self, df_sf, schemaForDeltaTable):
        from logging import getLogger, INFO

        logger = getLogger()
        # schemaForDeltaTable = self.delta_schema
        # deltaTable = {schemaForDeltaTable} + "." + {self.table}
        try:
            df_sf.write.format("delta").mode("overwrite").saveAsTable(
                f"{schemaForDeltaTable}.{self.table}"
            )
            sf_row_count = df_sf.count()
            df_delta = self.spark.read.table(f"{schemaForDeltaTable}.{self.table}")
            delta_row_count = df_delta.count()
            if sf_row_count == delta_row_count:
                logger.info("All records have been ingested to delta lake")
                self.logRun(
                    process="Delta Writer -" + self.table,
                    table=self.table,
                    sf_row_count=sf_row_count,
                    delta_row_count=delta_row_count,
                    status="Succeeded",
                    error="No error",
                    logTableName=f"{self.raw}.historical_run_details_from_sf",
                )
            else:
                logger.info("Records mismatch")
                self.logRun(
                    process="Delta Writer -" + self.table,
                    table=self.table,
                    sf_row_count=sf_row_count,
                    delta_row_count=delta_row_count,
                    status="Records mismatch",
                    error="Records mismatch",
                    logTableName=f"{self.raw}.historical_run_details_from_sf",
                )
        except Exception as e:
            self.logRun(
                process="Delta Writer -" + self.table,
                table=self.table,
                sf_row_count=None,
                delta_row_count=None,
                status="Failed",
                error=str(e),
                logTableName=f"{self.raw}.historical_run_details_from_sf",
            )
            raise e
