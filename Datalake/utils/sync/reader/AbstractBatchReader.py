from abc import ABC, abstractmethod
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import ByteType, DecimalType, IntegerType, LongType, ShortType

from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig


class AbstractBatchReader(ABC):
    def __init__(self, config: DateRangeBatchConfig):
        self.config = config

    def _convert_decimal_to_int_types(self, df: DataFrame) -> DataFrame:
        print(
            "AbstractBatchReader::_convert_decimal_to_int_types::converting decimal types"
        )
        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                if field.dataType.scale == 0:
                    if 0 < field.dataType.precision <= 2:
                        print(f"converting decimal to ByteType for field {field.name}")
                        df = df.withColumn(field.name, col(field.name).cast(ByteType()))
                    elif 2 < field.dataType.precision <= 5:
                        print(f"converting decimal to ShortType for field {field.name}")
                        df = df.withColumn(
                            field.name, col(field.name).cast(ShortType())
                        )
                    elif 5 < field.dataType.precision <= 9:
                        print(f"converting decimal to Byte for field {field.name}")
                        df = df.withColumn(
                            field.name, col(field.name).cast(IntegerType())
                        )
                    elif 10 <= field.dataType.precision <= 18:
                        print(f"converting decimal to LongType for field {field.name}")
                        df = df.withColumn(field.name, col(field.name).cast(LongType()))
            return df

    @abstractmethod
    def next(self) -> DataFrame:
        pass
