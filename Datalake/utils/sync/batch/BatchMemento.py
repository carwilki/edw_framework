from Datalake.utils.sync.batch.BatchManager import BatchReaderSourceType
from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig


from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass()
class BatchMemento(object):
    batch_id: str
    env: str
    source_table_fqn: str
    target_table_fqn: str
    source_type: BatchReaderSourceType
    source_filter: str | None = None
    keys: list[str] = field(default_factory=list)
    excluded_columns: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)
    start_dt: datetime
    end_dt: datetime
    current_dt: datetime
    interval: timedelta = field(default_factory=lambda: timedelta(weeks=1))

    def __str__(self) -> str:
        print(
            f"""********************************
            BatchMemento:
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
        )

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__ = d

    def to_config(self):
        return DateRangeBatchConfig(
            batch_id=self.batch_id,
            env=self.env,
            source_type=self.source_type,
            source_table_fqn=self.source_table_fqn,
            target_table_fqn=self.target_table_fqn,
            source_filter=self.source_filter,
            excluded_columns=self.excluded_columns,
            date_columns=self.date_columns,
            start_dt=self.start_dt,
            end_dt=self.end_dt,
            current_dt=self.current_dt,
            interval=self.interval,
        )