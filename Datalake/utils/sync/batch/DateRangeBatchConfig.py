from dataclasses import dataclass, field
from datetime import datetime, timedelta

from Datalake.utils.sync.batch.BatchReaderSourceType import BatchReaderSourceType


@dataclass()
class DateRangeBatchConfig(object):
    """
    This dataclass is used to store the configuration information for the script.
    The configuration information includes the name of the table to be read from the source system,
    the name of the table to be loaded into the target system, and the type of source system being read from.
    """

    batch_id: str
    env: str
    source_type: BatchReaderSourceType
    source_table_fqn: str
    target_table_fqn: str
    start_dt: datetime
    end_dt: datetime
    current_dt: datetime | None = field(default=None, compare=False)
    source_filter: str | None = None
    keys: list[str] = field(default_factory=list)
    excluded_columns: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)
    interval: timedelta = field(default_factory=lambda: timedelta(weeks=1))
    partition_colunm: str | None = None
    
    def __str__(self) -> str:
        return f"""********************************
            DateRangeBatchConfig:
                batch_id:           {self.batch_id}
                env:                {self.env}
                source_table_fqn:   {self.source_table_fqn}
                target_table_fqn:   {self.target_table_fqn}
                source_type:        {self.source_type}
                source_filter:      {self.source_filter}
                keys:               {self.keys}
                excluded_columns:   {self.excluded_columns}
                date_columns:       {self.date_columns}
                start_dt:           {self.start_dt}
                end_dt:             {self.end_dt}
                current_dt:         {self.current_dt}
                interval:           {self.interval}
            ********************************"""

    @classmethod
    def empty(cls) -> "DateRangeBatchConfig":
        return cls(
            batch_id="",
            env="",
            source_type=BatchReaderSourceType.SNOWFLAKE,
            source_table_fqn="",
            target_table_fqn="",
            start_dt=datetime.now(),
            end_dt=datetime.now(),
            current_dt=datetime.now(),
        )
