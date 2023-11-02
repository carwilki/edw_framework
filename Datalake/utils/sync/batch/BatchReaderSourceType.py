from enum import Enum


class BatchReaderSourceType(Enum):
    """
    This enum defines the different types of source systems that can be read from by the script.
    """

    SNOWFLAKE = "snowflake"
    NETEZZA = "netezza"
