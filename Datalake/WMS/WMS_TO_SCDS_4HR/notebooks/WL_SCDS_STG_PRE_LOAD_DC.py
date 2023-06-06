import argparse
from Datalake.WMS.WMS_TO_SCDS_4HR.notebooks.m_WM_E_Consol_Perf_Smry_PRE import perf_smry
from Datalake.WMS.WMS_TO_SCDS_4HR.notebooks.m_WM_E_Dept_PRE import dept_pre
from Datalake.WMS.WMS_TO_SCDS_4HR.notebooks.m_WM_Ucl_User_PRE import user_pre
from logging import getLogger, INFO

parser = argparse.ArgumentParser()
logger = getLogger()

parser.add_argument("DC_NBR", type=str, help="DC number")
parser.add_argument("env", type=str, help="Env Variable")
args = parser.parse_args()
dcnbr = args.DC_NBR
env = args.env

if dcnbr is None or dcnbr == "":
    raise ValueError("DC_NBR is not set")

if env is None or env == "":
    raise ValueError("env is not set")


####################################################################
# foreach mapping in maplet/worklet call the corresponding notebook
# that is created.
# main section #
####################################################################

user_pre(dcnbr, env)
logger.info("m_WM_Ucl_User_PRE executed")
dept_pre(dcnbr, env)
logger.info("m_WM_E_Dept_PRE executed")
perf_smry(dcnbr, env)
logger.info("m_WM_E_Consol_Perf_Smry_PRE executed")
