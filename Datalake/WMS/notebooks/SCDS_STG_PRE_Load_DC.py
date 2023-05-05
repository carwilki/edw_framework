import dbutils
from datetime import datetime

# COMMAND ----------
# Variable_declaration_comment
# Read in job variables
# read_infa_paramfile('', 'm_WM_E_Consol_Perf_Smry_PRE') ProcessingUtils
dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='01/01/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')
dbutils.widgets.text(name='Catalog', defaultValue='dev')

starttime = datetime.now() #start timestamp of the script
catalog = dbutils.widgets.get('catalog', as_type=str)
dcnbr = dbutils.widgets.get('DC_NBR', as_type=str)
prev_run_dt = dbutils.widgets.get('Prev_Run_Dt', as_type=str)	
initial_load = dbutils.widgets.get('Initial_Load', as_type=str)

# COMMAND ----------
dbutils.notebook.run('m_WM_Ucl_User_PRE.py','60', {
                             "DC_NBR":f"{dcnbr}",
                             "Prev_Run_Dt":f"{prev_run_dt}",
                             "Initial_Load":f"{initial_load}",
                             "Catalog":f"{catalog}"
                            })

dbutils.notebook.run('m_WM_E_Dept_PRE.py','60',
                            {
                             "DC_NBR":f"{dcnbr}",
                             "Prev_Run_Dt":f"{prev_run_dt}",
                             "Initial_Load":f"{initial_load}",
                             "Catalog":f"{catalog}"
                            })

dbutils.notebook.run('m_WM_E_Consol_Perf_Smry_PRE.py','60', {
                            "DC_NBR":f"{dcnbr}",
                             "Prev_Run_Dt":f"{prev_run_dt}",
                             "Initial_Load":f"{initial_load}",
                             "Catalog":f"{catalog}"
                            })
