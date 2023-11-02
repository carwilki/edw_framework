from automapper import mapper
from Datalake.utils.sync.batch.BatchMemento import BatchMemento

from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig


def toBatchMemento(config: DateRangeBatchConfig) -> BatchMemento:
    return mapper.to(BatchMemento).map(config)


def toDateRangeBatchConfig(memento: BatchMemento) -> DateRangeBatchConfig:
    return mapper.to(DateRangeBatchConfig).map(memento)
