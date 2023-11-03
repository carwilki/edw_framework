
from datetime import datetime, timedelta

from pydantic import BaseModel

from Datalake.utils.sync.batch.BatchReaderSourceType import BatchReaderSourceType


class BatchMemento(BaseModel):
    batch_id: str
    env: str
    source_table_fqn: str
    target_table_fqn: str
    source_type: BatchReaderSourceType
    start_dt: datetime
    end_dt: datetime
    current_dt: datetime
    source_filter: str | None = None
    keys: list[str] = []
    excluded_columns: list[str] = []
    date_columns: list[str] = []
    interval: timedelta = timedelta(weeks=1)

    def __str__(self) -> str:
        return f"""********************************
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

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__ = d
