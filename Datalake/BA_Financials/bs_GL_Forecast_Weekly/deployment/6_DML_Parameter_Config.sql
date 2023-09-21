INSERT INTO raw.parameter_config (parameter_file_name,parameter_section,parameter_key,parameter_value)
VALUES('BA_Financials_Parameter.prm','BA_Financials.WF:bs_GL_Forecast_Weekly','source_file','BPC_PLAN_FORECAST.csv');


INSERT INTO raw.parameter_config (parameter_file_name,parameter_section,parameter_key,parameter_value)
VALUES('BA_Financials_Parameter.prm','BA_Financials.WF:bs_GL_Forecast_Weekly','source_bucket','gs://petm-bdpl-qa-raw-p1-gcs-gbl/nas/financials/bpc_plan_forecast/');

