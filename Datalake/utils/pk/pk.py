from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

class DuplicateSourceKeyException(Exception):
    """_summary_

    Args:
        Exception (_type_): _description_
    """

    def __init__(self, values: list, primary_keys: list[str]):
        super(DuplicateSourceKeyException, self).__init__()
        self.args(values, primary_keys)


class DuplicateTargetKeyException(Exception):
    """_summary_

    Args:
        Exception (_type_): _description_
    """

    def __init__(self, values: list, primary_keys: list[str]):
        super(DuplicateTargetKeyException, self).__init__()
        self.args(values, primary_keys)


class DuplicateChecker(object):
    """
    Checks for duplicates and throws DuplicateKeyException if there are duplicates.
    Raises:
        DuplicateKeyException: Indicates there are duplicate primary keys.will have a max of 10 example
        keys
    """

    @classmethod
    def check_for_duplicate_primary_keys(
        cls,
        values: DataFrame,
        primary_keys: list[str],
    ) -> None:
        """check_for_duplicate_primary_keys unions the given values against the values in
        existing_table_fqn. the final table is the dupe checked using a group by the primary keys.
        Args:
            spark (SparkSession): Spark Session to use to execute the query
            exiting_table_fqn (str): the fully qualified name of the table that we want to add the new values to
            new_values (DataFrame): a data frame containing the new values that we want to add existing table
            primary_keys (list[str]): List of the primary keys to identify all records in the table

        Raises:
            DuplicateKeyException: thrown if there are duplicate primary keys. will have a max of 10 example keys
        """
        ret = (
            values.select(*primary_keys)
            .groupby(*primary_keys)
            .count().filter(col("count") > 1).collect()
        )
        
        if len(ret) > 0:
            raise DuplicateSourceKeyException(ret[:10], primary_keys)
