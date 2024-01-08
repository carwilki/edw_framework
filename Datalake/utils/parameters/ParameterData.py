from pyspark.sql import SparkSession
from Datalake.utils.genericUtilities import getEnvPrefix


class ParameterFile(dict[str, dict[str, str]]):
    """
    this class is used as a container for the parameter_config table values.
    """

    def __init__(self):
        super(ParameterFile, self).__init__()

    def get_source_buckets_archive_pairs(self) -> (str, str):
        for _, v in self.items():
            s = v.get("source_bucket")
            a = v.get("archive_bucket")
            if s is not None and a is not None:
                yield (s, a)


class ParameterData:
    """
    This class is used to access the parameter_config table.
    """

    parameter_section = "parameter_section"
    parameter_key = "parameter_key"
    parameter_value = "parameter_value"

    def __init__(self, env, spark: SparkSession) -> None:
        self.env = env
        self.spark = spark
        prefix = getEnvPrefix(self.env)
        self.table = "parameter_config"
        self.schema = "raw"
        self.table_fqn = f"{prefix}{self.schema}.{self.table}"

    def get_parameter_file(self, parameter_file_name) -> ParameterFile:
        """
        gets a named paramter file from the parameter_config table. the table store the kv pairs
        as single rows. The dict is built up by loop through all the rows in the table and adding
        them to the params dictionary.
        """
        rows = self.spark.sql(
            f"select * from {self.table_fqn} where parameter_file_name = '{parameter_file_name}'"
        ).collect()
        params: ParameterFile = ParameterFile()
        # loop through all the rows in the table
        for row in rows:
            # if the row's parameter_section is not in the params dictionary, create it
            if params.get(row[self.parameter_section]) is None:
                params[row[self.parameter_section]] = {}
            # add the row's parameter_key and parameter_value to the params dictionary
            params[row[self.parameter_section]][row[self.parameter_section]] = row[
                self.parameter_value
            ]

        return params

    def add_parameter(
        self, id, parameter_file_name, parameter_section, parameter_key, parameter_value
    ):
        self.spark.sql(
            f"insert into table {self.table} values ({id},'{parameter_file_name}','{parameter_section}','{parameter_key}','{parameter_value}')"
        )

    def remove_parameter(
        self, id, parameter_file_name, parameter_section, parameter_key
    ):
        self.spark.sql(
            f"""
            delete from table {self.table}
            where id = {id} and parameter_file_name = '{parameter_file_name}'
            and parameter_section = '{parameter_section}' and parameter_key = '{parameter_key}'"""
        )

    def update_parameter(
        self, id, parameter_file_name, parameter_section, parameter_key, parameter_value
    ):
        self.spark.sql(
            f"""
            update table {self.table}
            set parameter_value = '{parameter_value}'
            where id = {id} and parameter_file_name = '{parameter_file_name}'
            and parameter_section = '{parameter_section}' and parameter_key = '{parameter_key}'"""
        )
