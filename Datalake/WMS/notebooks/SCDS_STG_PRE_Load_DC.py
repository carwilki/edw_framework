# Databricks notebook source
dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='01/01/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')

# COMMAND ----------
dbutils.notebook.run('m_WM_Ucl_User_PRE.py','60', {
                             "DC_NBR":f"{dc}",
                             "Prev_Run_Dt":f"{dbutils.get('Prev_Run_Dt')}",
                             "Initial_Load":f"{dbutils.get('Prev_Run_Dt')}"
                            })

dbutils.notebook.run('m_WM_E_Dept_PRE.py','60',
                            {
                             "DC_NBR":f"{dc}",
                             "Prev_Run_Dt":f"{dbutils.get('Prev_Run_Dt')}",
                             "Initial_Load":f"{dbutils.get('Prev_Run_Dt')}"
                            })

dbutils.notebook.run('m_WM_E_Consol_Perf_Smry_PRE.py','60', {
                             "DC_NBR":f"{dc}",
                             "Prev_Run_Dt":f"{dbutils.get('Prev_Run_Dt')}",
                             "Initial_Load":f"{dbutils.get('Prev_Run_Dt')}"
                            })
