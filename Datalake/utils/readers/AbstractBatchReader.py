from abc import ABC, abstractmethod
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, ByteType, ShortType, IntegerType, LongType


class AbstractBatchReader(ABC):
    def _convert_decimal_to_int_types(df) -> DataFrame:
        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                if field.dataType.scale == 0:
                    if 0 < field.dataType.precision <= 2:
                        df = df.withColumn(field.name, col(field.name).cast(ByteType()))
                    elif 2 < field.dataType.precision <= 5:
                        df = df.withColumn(
                            field.name, col(field.name).cast(ShortType())
                        )
                    elif 5 < field.dataType.precision <= 9:
                        df = df.withColumn(
                            field.name, col(field.name).cast(IntegerType())
                        )
                    elif 10 <= field.dataType.precision <= 18:
                        df = df.withColumn(field.name, col(field.name).cast(LongType()))
            return df

    @abstractmethod
    def next(self, dt: datetime) -> DataFrame:
        pass
