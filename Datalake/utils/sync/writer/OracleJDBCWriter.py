from pyspark.sql import SparkSession, DataFrame


class OracleJDBCWriter:
    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        username: str,
        password: str,
        database: str,
        schema: str,
        source_table_fqn: str,
        target_table_fqn: str,
        ingest_sproc: str,
    ):
        self.spark: SparkSession = spark
        self.jdbc_url: str = jdbc_url
        self.username: str = username
        self.password: str = password
        self.database: str = database
        self.schema: str = schema
        self.source_table_fqn: str = source_table_fqn
        self.target_table_fqn: str = target_table_fqn
        self.ingest_sproc: str = ingest_sproc

        self.driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager

    def write(self, df: DataFrame) -> None:
        (self.spark.table(self.source_table_fqn).write.format("jdbc"))
        pl_block = f"""
        begin 
            {self.ingest_sproc};
        end;
        """
        count_query = f"select count(*) from {self.target_table_fqn}"

        self.spark.table(self.source_table_fqn).write.format("jdbc").option(
            "url", self.jdbc_url
        ).option("dbtable", self.target_table_fqn).option("user", self.username).option(
            "password", self.password
        ).option(
            "numPartitions", 16
        ).option(
            "driver", "oracle.jdbc.OracleDriver"
        ).option(
            "oracle.jdbc.timezoneAsRegion", "false"
        )
        df = (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .option("oracle.jdbc.timezoneAsRegion", "false")
            .option("sessionInitStatement", pl_block)
            .option("dbtable", count_query)
            .option("user", self.username)
            .option("password", self.password)
            .load()
        )

        return df
