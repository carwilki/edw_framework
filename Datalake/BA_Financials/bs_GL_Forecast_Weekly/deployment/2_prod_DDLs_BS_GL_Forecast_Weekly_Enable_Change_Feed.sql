--enables the cdf feature for the following tables
use legacy;
ALTER TABLE GL_PLAN_FORECAST_MONTH SET TBLPROPERTIES (delta.enableChangeDataFeed = true);