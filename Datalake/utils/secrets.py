from retry import retry
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

sc = SparkSession.getActiveSession()
dbutils = DBUtils(sc)


@retry(tries=10, delay=1, backoff=2, jitter=(1, 5), exceptions=Py4JJavaError)
def get(scope: str, key: str):
    """
    implements backoff and retry logic for dbutils.secrets.get
    """
    return dbutils.secrets.get(scope=scope, key=key)
