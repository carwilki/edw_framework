from pyspark.sql import SparkSession, DataFrame


class OracleWriter:
    def __init__(
        self,
        spark: SparkSession,
        statement: str,
        statement_type: str,
        jdbc_url: str,
        username: str,
        password: str,
        database: str,
        schema: str,
        target_table_fqn: str,
        source_table_fqn: str,
    ):
        self.spark = spark
        self.statement = statement
        self.statement_type = statement_type
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password
        self.database = database
        self.schema = schema
        self.target_table_fqn = target_table_fqn
        self.source_table_fqn = source_table_fqn
        if self.statement_type not in ["sp", "sql"]:
            raise ValueError(f"Unsupported statement type: {self.statement_type}")

        driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
        con = driver_manager.getConnection(jdbc_url, username, password)
        con.p
        
