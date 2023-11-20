from logging import INFO, getLogger

import pyspark.sql.functions as F
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

import Datalake.utils.secrets as secrets

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


def or_kro_read(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.238"
        portnumber = "1800"
        db = "krop1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key="temp_krop1_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = secrets.get(scope="SVC_BD_ORA_P_READ", key="username")
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key="krop1_password")
        hostname = "172.20.89.166"
        portnumber = "1810"
        db = "krop1"
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"
        return (username, password, connection_string)


def esdh_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="SVC_BD_SQL_READ", key="username")
        password = secrets.get(scope="SVC_BD_SQL_READ", key="esdh_password")
        hostname = "172.20.89.138"
        portnumber = "1840"
        db = "EnterpriseSiteDataHub"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)
    raise Exception(f"Environment {env} is not supported")



def store_loc_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="svc_bd_sql_p_read", key="username")
        password = secrets.get(scope="svc_bd_sql_p_read", key="store_locator_password")
        hostname = "10.116.133.31"
        portnumber = "1433"
        db = "StoreLocator"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)
    raise Exception(f"Environment {env} is not supported")


def salon_call_log_daily_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = dbutils.secrets.get(scope="svc_bd_sql_np_write", key="mtx_username")
        password = dbutils.secrets.get(scope="svc_bd_sql_np_write", key="mtx_password")
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
        username = secrets.get(scope="svc_bd_sql_p_write", key="username")
        password = secrets.get(scope="svc_bd_sql_p_write", key="mtx_password")
        hostname = "172.20.89.186"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")


def pettraining_prd_sqlServer_training(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="SVC_BD_SQL_P_READ", key="username")
        password = secrets.get(scope="SVC_BD_SQL_P_READ", key="training_password")
        hostname = "10.116.133.31"
        portnumber = "1433"
        db = "Training"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")


def pettraining_prd_sqlServer_trainingSched(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="SVC_BD_SQL_P_READ", key="username")
        password = secrets.get(scope="SVC_BD_SQL_P_READ", key="trnsched_prd_password")
        hostname = "172.20.89.184"
        portnumber = "1840"
        db = "TrainingSched_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")


def timesmart_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":      
        # Access time_tracking db through a linked server in MTX_PRD
        # username, password, hostname        
        username = secrets.get(scope="SVC_BD_SQL_P_READ", key="username")
        password = secrets.get(scope="SVC_BD_SQL_P_READ", key="mtx_password")
        hostname = "172.20.89.186"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""
        linked_server = "timetracking"

        return (username, password, connection_string, linked_server)

    raise Exception(f"Environment {env} is not supported")

def UserDataFeed_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="SVC_BD_SQL_P_READ", key="username")
        password = secrets.get(scope="SVC_BD_SQL_P_READ", key="udf_password")
        hostname = "172.20.89.138"
        portnumber = "1840"
        db = "UserDataFeed"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")

def petHotel_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="SVC_BD_SQL_P_READ", key="username")
        password = secrets.get(scope="SVC_BD_SQL_P_READ", key="ereserv_password")
        hostname = "172.20.89.184"
        portnumber = "1840"
        db = "eReservations"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")


def SalonAcademy_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="svc_bd_sql_p_read", key="username")
        password = secrets.get(scope="svc_bd_sql_p_read", key="salon_password")
        hostname = "172.20.89.187"
        portnumber = "1840"
        db = "SalonAcademy"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")


def PetTraining_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="svc_bd_sql_p_read", key="username")
        password = secrets.get(scope="svc_bd_sql_p_read", key="pettrn_password")
        hostname = "172.20.89.187"
        portnumber = "1840"
        db = "PetTraining"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")


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


def getMaxDate(refine_table_name, schema):
    import datetime as dt
    from logging import getLogger

    logger = getLogger()
    logger.info("getMaxDate funcation is getting executed")

    if "dev" in schema:
      metadataSchema = "dev_stranger_things"
    elif "qa" in schema:
      metadataSchema = "qa_stranger_things"
    else:
      metadataSchema = "stranger_things"

    metadata_df = spark.table(f"{metadataSchema}.ingestion_metadata")
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


def or_kro_read_krap1(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.238"
        portnumber = "1800"
        db = "krap1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"temp_krap1_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        username = secrets.get(scope="SVC_BD_ORA_P_READ", key="username")
        hostname = "172.20.89.168"
        portnumber = "1812"
        db = "KRAP1"
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key="krap1_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)


def or_kro_read_edhp1(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.238"
        portnumber = "1800"
        db = "edhp1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"temp_edhp1_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    if env.lower() == "prod":
        hostname = "172.20.89.163"
        portnumber = "1804"
        db = "EDHP1"
        username = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"username")
        password = secrets.get(scope="SVC_BD_ORA_P_READ", key=f"edhp1_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")


def evdh_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="svc_bd_sql_p_read", key="username")
        password = secrets.get(scope="svc_bd_sql_p_read", key="evdh_password")
        hostname = "172.20.89.138"
        portnumber = "1840"
        db = "EnterpriseVendorDataHub"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)
    raise Exception(f"Environment {env} is not supported")


def Get_Smart_Training_prd_sqlServer(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = secrets.get(scope="svc_bd_sql_np_read", key="mtx_username")
        password = secrets.get(scope="svc_bd_sql_np_read", key="mtx_password")
        hostname = "172.17.89.188"
        portnumber = "1840"
        db = "MTX_PRD"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    if env.lower() == "prod":
        # username, password, hostname
        username = secrets.get(scope="svc_bd_sql_p_read", key="username")
        password = secrets.get(scope="svc_bd_sql_p_read", key="getsmarttrn_password")
        hostname = "172.20.89.187"
        portnumber = "1840"
        db = "GetSmartTraining_V1"
        connection_string = f"""jdbc:sqlserver://{hostname}:{portnumber};databaseName={db};encrypt=true;trustServerCertificate=true;"""

        return (username, password, connection_string)

    raise Exception(f"Environment {env} is not supported")


def ckb_prd_sqlServer(env):
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
        password = dbutils.secrets.get(scope="svc_bd_sql_p_read", key="ckb_password")
        hostname = "172.20.89.153"
        portnumber = "1840"
        db = "CKB_PRD"
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

