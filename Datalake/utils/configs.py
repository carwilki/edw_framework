import pyspark.sql.functions as F
import Datalake.utils.secrets as secrets
from logging import getLogger, INFO
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

logger = getLogger()
logger.setLevel(INFO)

spark: SparkSession = SparkSession.getActiveSession()
dbutils: DBUtils = DBUtils(spark)


def dc10(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.175"
        portnumber = "1810"
        db = "Wmdc10q1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.160"
        portnumber = "1810"
        db = "WMDC10P1"
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)


def dc12(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.186"
        portnumber = "1812"
        db = "Wmdc12q1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.161"
        portnumber = "1812"
        db = "WMDC12P1"
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)


def dc14(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.170"
        portnumber = "1814"
        db = "wmdc14q1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.159"
        portnumber = "1814"
        db = "WMDC14P1"
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)


def dc22(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.175"
        portnumber = "1822"
        db = "Wmdc22q1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.160"
        portnumber = "1822"
        db = "WMDC22P1"
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)


def dc36(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.170"
        portnumber = "1836"
        db = "wmdc36q1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.159"
        portnumber = "1836"
        db = "WMDC36P1"
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)


def dc38(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.186"
        portnumber = "1838"
        db = "Wmdc38q1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.161"
        portnumber = "1838"
        db = "WMDC38P1"
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)


def dc41(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.175"
        portnumber = "1841"
        db = "Wmdc41q1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.160"
        portnumber = "1841"
        db = "WMDC41P1"
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)


def mtx_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = dbutils.secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = dbutils.secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = (
            "jdbc:sqlserver://"
            + hostname
            + ":"
            + portnumber
            + ";databaseName="
            + db
            + ";encrypt=true;trustServerCertificate=true;"
        )
        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = dbutils.secrets.get(scope="SVC_BD_SQL_READ", key="username")
        password = dbutils.secrets.get(scope="SVC_BD_SQL_READ", key="esdh_password")
        hostname = "172.20.89.138"
        portnumber = "1840"
        db = "EnterpriseSiteDataHub"
        connection_string = (
            f"jdbc:sqlserver://"
            + hostname
            + ":"
            + portnumber
            + ";databaseName="
            + db
            + ";encrypt=true;trustServerCertificate=true;"
        )
        return (username, password, connection_string)

def SalonAcademy_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = dbutils.secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = dbutils.secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = (
            "jdbc:sqlserver://"
            + hostname
            + ":"
            + portnumber
            + ";databaseName="
            + db
            + ";encrypt=true;trustServerCertificate=true;"
        )
        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = dbutils.secrets.get(scope="svc_bd_sql_p_read", key="username")
        password = dbutils.secrets.get(scope="svc_bd_sql_p_read", key="salon_password")
        hostname = "172.20.89.187"
        portnumber = "1840"
        db = "SalonAcademy"
        connection_string = (
            f"jdbc:sqlserver://"
            + hostname
            + ":"
            + portnumber
            + ";databaseName="
            + db
            + ";encrypt=true;trustServerCertificate=true;"
        )
        return (username, password, connection_string)    
    
def PetTraining_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = dbutils.secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = dbutils.secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = (
            "jdbc:sqlserver://"
            + hostname
            + ":"
            + portnumber
            + ";databaseName="
            + db
            + ";encrypt=true;trustServerCertificate=true;"
        )
        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = dbutils.secrets.get(scope="SVC_BD_SQL_READ", key="username")
        password = dbutils.secrets.get(scope="SVC_BD_SQL_READ", key="esdh_password")
        #hostname = "172.20.89.138"
        #portnumber = "1840"
        db = "PetTraining"
        connection_string = (
            f"jdbc:sqlserver://"
            + hostname
            + ":"
            + portnumber
            + ";databaseName="
            + db
            + ";encrypt=true;trustServerCertificate=true;"
        )
        return (username, password, connection_string)        

def getConfig(DC_NBR, env):
    logger.info("getConfig function is getting executed")
    select = {
        "dc10": dc10(env),
        "dc12": dc12(env),
        "dc14": dc14(env),
        "dc22": dc22(env),
        "dc36": dc36(env),
        "dc38": dc38(env),
        "dc41": dc41(env),
    }
    return select.get(DC_NBR)


# def getMaxDateOld(refine_table_name, schema):
#     from logging import getLogger, INFO

#     logger = getLogger()
#     logger.info("getMaxDate funcation is getting executed")
#     if refine_table_name == "WM_E_DEPT":
#         maxDateQuery = f"""select WM_CREATE_TSTMP,WM_MOD_TSTMP,WM_CREATED_TSTMP,greatest(coalesce(WM_CREATE_TSTMP,WM_MOD_TSTMP,WM_CREATED_TSTMP),coalesce(WM_MOD_TSTMP,WM_CREATE_TSTMP,WM_CREATED_TSTMP) ,coalesce(WM_CREATED_TSTMP,WM_CREATE_TSTMP,WM_MOD_TSTMP) ) as max_date from {schema}.{refine_table_name}"""
#     elif refine_table_name == "WM_UCL_USER":
#         maxDateQuery = f"""select greatest(coalesce(WM_CREATED_TSTMP,WM_LAST_UPDATED_TSTMP),coalesce(WM_LAST_UPDATED_TSTMP,WM_CREATED_TSTMP) ) as max_date from {schema}.{refine_table_name}"""
#     elif refine_table_name == "WM_E_CONSOL_PERF_SMRY":
#         maxDateQuery = f"""select greatest(coalesce(WM_CREATE_TSTMP,WM_MOD_TSTMP),coalesce(WM_MOD_TSTMP,WM_CREATE_TSTMP) ) as max_date from {schema}.{refine_table_name}"""
#     df = spark.sql(maxDateQuery)
#     maxDate = df.select(F.max(F.col("max_date"))).first()[0]
#     maxDate = maxDate.strftime("%Y-%m-%d")
#     print("Max date::", maxDate)
#     logger.info("returnning max date")
#     return maxDate


def getMaxDate(refine_table_name, schema):
    from logging import getLogger
    import datetime as dt

    logger = getLogger()
    logger.info("getMaxDate funcation is getting executed")
    metadata_df = spark.table(f"{schema}.ingestion_metadata")
    try:
        columns = (
            metadata_df.select("timestamp_columns")
            .where("lower(table_name)='" + refine_table_name.lower() + "'")
            .first()[0]
        )
    except Exception as e:
        logger.info(f"{refine_table_name} not added to ingestion_metadata table")
        raise e

    columnsList = columns.split(",")
    print(columnsList)

    row_cnt = spark.sql(f"select count(*) from {schema}.{refine_table_name}").first()[0]

    logger.info(f"Row_count for {refine_table_name} table is {row_cnt}")
    if columns == "" or row_cnt == 0:
        logger.info("Setting maxDate as currentDate!")
        maxDate = dt.datetime.today() - dt.timedelta(days=1)

    else:
        if len(columnsList) > 4 or len(columnsList) == 0:
            raise Exception(
                "Invalid number of columns,cannot generate getMaxDate query!"
            )
        else:
            logger.info("Generating query for getting the max date!")
            if len(columnsList) == 1:
                maxDateQuery = f"select max({columns})"
            else:
                maxDateQuery = f"select greatest( coalesce({columns}),coalesce("
                if len(columnsList) == 2:
                    maxDateQuery = (
                        maxDateQuery + f"{columnsList[1]},{columnsList[0]}) )"
                    )
                elif len(columnsList) == 3:
                    maxDateQuery = (
                        maxDateQuery
                        + f"{columnsList[1]},{columnsList[0]},{columnsList[2]}) , coalesce({columnsList[2]},{columnsList[0]},{columnsList[1]} ) )"
                    )
                elif len(columnsList) == 4:
                    maxDateQuery = (
                        maxDateQuery
                        + f"{columnsList[1]},{columnsList[0]},{columnsList[2]},{columnsList[3]}) , coalesce({columnsList[2]},{columnsList[0]},{columnsList[1]},{columnsList[3]} ), coalesce({columnsList[3]},{columnsList[0]},{columnsList[1]},{columnsList[2]} ) )"
                    )

            maxDateQuery = (
                maxDateQuery + f" as max_date from {schema}.{refine_table_name}"
            )
            print(maxDateQuery)
            df = spark.sql(maxDateQuery)
            maxDate = df.select(F.max(F.col("max_date"))).first()[0]

    maxDate = maxDate.strftime("%Y-%m-%d")
    print(f"Max date: {maxDate}")
    logger.info("Returning max date")
    return maxDate
