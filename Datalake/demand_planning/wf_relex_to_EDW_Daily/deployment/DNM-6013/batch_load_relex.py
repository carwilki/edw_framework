# Databricks notebook source
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, ByteType, ShortType, IntegerType, LongType
import pandas
from Datalake.utils.genericUtilities import getSfCredentials


def _convert_decimal_to_int_types(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            if field.dataType.scale == 0:
                if 0 < field.dataType.precision <= 2:
                    df = df.withColumn(field.name, col(field.name).cast(ByteType()))
                elif 2 < field.dataType.precision <= 5:
                    df = df.withColumn(field.name, col(field.name).cast(ShortType()))
                elif 5 < field.dataType.precision <= 9:
                    df = df.withColumn(field.name, col(field.name).cast(IntegerType()))
                elif 10 <= field.dataType.precision <= 18:
                    df = df.withColumn(field.name, col(field.name).cast(LongType()))
        return df


def _read_from_netezza(query: str) -> DataFrame:
    dbutils = DBUtils(spark)
    username = dbutils.secrets.get(scope="HAATHEELOADER-mako8", key="username")
    password = dbutils.secrets.get(scope="HAATHEELOADER-mako8", key="password")
    netezza_jdbc_url = "jdbc:netezza://172.20.73.73:5480/"
    netezza_jdbc_user = username
    netezza_jdbc_password = password
    netezza_jdbc_driver = "org.netezza.Driver"
    netezza_jdbc_num_part = 9
    sql = f"""({query}) as data"""

    reader_options = {
        "driver": netezza_jdbc_driver,
        "url": f"""{netezza_jdbc_url}EDW_PRD;""",
        "dbtable": f"{sql}",
        "fetchsize": 10_000,
        "user": netezza_jdbc_user,
        "password": netezza_jdbc_password,
        "numPartitions": netezza_jdbc_num_part,
    }

    df = spark.read.format("jdbc").options(**reader_options).load()

    df = _convert_decimal_to_int_types(df)
    return df.repartition(50)


def _read_from_snowflake(sql) -> DataFrame:
    sfOptions = getSfCredentials()
    df = spark.read.format("snowflake").options(**sfOptions).option("query", sql).load()
    df = _convert_decimal_to_int_types(df)
    return df


def _write_to_datalake(ts: str, tt: str, df: DataFrame):
    df.write.format("delta").mode("append").saveAsTable(f"{ts}.{tt}")


def _get_nz_query(schema: str, table: str, filter: str) -> str:
    SQL = f"""Select * from {schema}.{table}"""

    if filter is not None:
        SQL = SQL + f""" WHERE {filter}"""
    return SQL


def _get_snowflake_query(schema: str, table: str, filter: str) -> str:
    SQL = f"""Select * from {schema}.{table}"""

    if filter is not None:
        SQL = SQL + f""" WHERE {filter}"""
    return SQL


def _load_nz(dates: list[pandas.Timestamp], ss, st, ts, tt, date_feild: str) -> None:
    def _load_to_datalake(ss: str, st: str, filter: str, ts: str, tt: str):
        query = _get_nz_query(ss, ts, filter)
        df = _read_from_netezza(query)
        _write_to_datalake(ts, tt, df)

    for ts in dates:
        lb = ts.strftime("%Y-%m-%d")
        ub = (ts + timedelta(days=1)).date().strftime("%Y-%m-%d")
        _load_to_datalake(
            f"{ss}",
            f"{st}",
            f"{date_feild} between '{lb}' and '{ub}'",
            "{ts}",
            "dp_accuracy_day",
        )
        _load_to_datalake(
            "EDW_PRD",
            "dp_forecast_week_hist",
            f"SNAPSHOT_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_forecast_week_hist",
        )
        _load_to_datalake(
            "EDW_PRD",
            "dp_order_projection_day_hist",
            f"SNAPSHOT_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_order_projection_day_hist",
        )
        _load_to_datalake(
            "EDW_PRD",
            "dp_order_projection_week_hist",
            f"SNAPSHOT_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_order_projection_week_hist",
        )
        _load_to_datalake(
            "EDW_PRD",
            "dp_product_location_settings_hist",
            f"SNAPSHOT_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_accuracy_day",
        )


def _load_snow(dates: list[pandas.Timestamp]):
    def _load_to_datalake(ss: str, st: str, filter: str, ts: str, tt: str):
        query = _get_snowflake_query(ss, st, filter)
        df = _read_from_snowflake(query)
        _write_to_datalake(ts, tt, df)

    for ts in dates:
        lb = ts.date().strftime("%Y-%m-%d")
        ub = (ts.date() + timedelta(days=1)).strftime("%Y-%m-%d")

        _load_to_datalake(
            "PUBLIC",
            "dp_accuracy_day_nz",
            f"DAY_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_accuracy_day",
        )

        _load_to_datalake(
            "PUBLIC",
            "dp_forecast_week_hist_nz",
            f"SNAPSHOT_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_forecast_week_hist",
        )
        _load_to_datalake(
            "PUBLIC",
            "dp_order_projection_day_hist_nz",
            f"SNAPSHOT_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_order_projection_day",
        )
        _load_to_datalake(
            "PUBLIC",
            "dp_order_projection_week_hist_nz",
            f"SNAPSHOT_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_order_projection_week_hist",
        )
        _load_to_datalake(
            "PUBLIC",
            "dp_product_location_settings_hist_nz",
            f"SNAPSHOT_DT between '{lb}' and '{ub}'",
            "qa_refine",
            "dp_product_location_settings_hist",
        )


sf_min_date = datetime.datetime(2022, 1, 31)
nz_min_date = datetime.datetime(2020, 7, 21)
nz_max_date = datetime.datetime(2021, 12, 31)
spark = SparkSession.getActiveSession()

nz_dates = pandas.date_range(nz_min_date, nz_min_date, freq="d")
snow_dates = pandas.date_range(
    sf_min_date, datetime.now() + timedelta(days=5), freq="d"
)

_load_nz(nz_dates)
_load_snow(snow_dates)
