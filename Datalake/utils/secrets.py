from py4j.protocol import Py4JJavaError
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from retry import retry

sc = SparkSession.getActiveSession()
dbutils = DBUtils(sc)


@retry(tries=10, delay=1, backoff=2, jitter=(1, 5), exceptions=Py4JJavaError)
def get(scope: str, key: str):
    """
    implements backoff and retry logic for dbutils.secrets.get
    """
    return dbutils.secrets.get(scope=scope, key=key)
