from typing import Optional
from Datalake.utils.logger import getLogger
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from Datalake.utils.sync.cdc.CDCWriter import CDCWriter


class DeltaCDCWriter(CDCWriter):
    def __init__(
        self,
        env: str,
        spark: SparkSession,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
        source_catalog: str = None,
        primary_keys: str = None,
        update_excl_columns=[],
        update_option: str | None = None,
    ):
        super().__init__(
            env=env,
            spark=spark,
            source_schema=source_schema,
            source_table=source_table,
            target_schema=target_schema,
            target_table=target_database,
            target_catalog=target_table,
            source_catalog=source_catalog,
            primary_keys=primary_keys,
            update_excl_columns=update_excl_columns,
        )
        self.logger = getLogger()
        self.update_option = update_option

    def _overwrite_target(self, df: DataFrame):
        self.logger.info(
            f"""running overwrite on {self._getSourceFQN()} to {self._getTargetFQN()}"""
        )
        df.write.format("delta").mode("overwrite").save(self._getTargetFQN())

    def _append_target(self, df: DataFrame):
        self.logger.info(
            f"""running append on {self._getSourceFQN()} to {self._getTargetFQN()}"""
        )
        df.write.format("delta").mode("append").save(self._getTargetFQN())

    def _check_for_control_colunms(self, df: DataFrame):
        if not all(
            col_name in df.columns
            for col_name in ("_change_type", "_commit_version", "_commit_timestamp")
        ):
            raise Exception(
                "The dataframe is missing required columns for CDC _change_type, _commit_version, _commit_timestamp"
            )

    def _merge_target(self, df: DataFrame):
        self._check_for_control_colunms(df)
        self.logger.info(
            f"""running merge on {self._getSourceFQN()} to {self._getTargetFQN()}"""
        )
        delta = DeltaTable.forName(self.spark, self._getTargetFQN())
        fields = self._build_field_expression(df)
        delta = (
            delta.alias("target")
            .merge(source=df.alias("source"), condition=self._buildMergeCondition())
            .whenMatchedDelete(condition="source._change_type == delete")
            .whenMatchedUpdate(
                condition="source._change_type == update_postimage", set=fields
            )
            .whenNotMatchedInsert(
                condition="source._change_type == insert", values=fields
            )
        )
        delta.execute()

    def _build_field_expression(self, df: DataFrame) -> dict[str, str]:
        df = df.drop("_change_type", "_commit_version", "_commit_timestamp")
        ret = {}
        for col in df.columns:
            ret[col] = f"source.{col}"

    def _buildMergeCondition(self):
        keys = [f"target.{key} = updates.{key}" for key in self.primary_keys]
        return "and".join(keys)

    def _push(self, df: DataFrame) -> Optional[int]:
        if self.update_option is None:
            self._overwrite_target(df)
        elif self.update_option.lower() == "overwrite":
            self._overwrite_target(df)
        elif self.update_option.lower() == "append":
            self._append_target(df)
        elif self.update_option.lower() == "merge":
            self._merge_target(df)
        else:
            raise ValueError(f"update_option {self.update_option} is not supported")
