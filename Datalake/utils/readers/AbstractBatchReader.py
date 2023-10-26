from abc import ABC
from datetime import datetime
from pyspark.sql import DataFrame


class AbstractBatchReader(ABC):
    def read(self, dt: datetime) -> DataFrame:
        pass
