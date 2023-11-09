# IMPORTANT
These Jobs should be run in continuous mode

After the jobs have been installed successfully unpause them so that they will start to run in the background.
Once the jobs have loaded all of the data then we need to disable them

## Details

**Installation Script**
1) Ensure that the nzmigration repo is deployed to the prod workspace. 
    - The repo needs to be named deployed to /Repos/nz-databricks-migration/nz-databricks-migration/
    - this is where the shared requirments scripts will be ref for bootstrapping libs for the jobs.
2) 1_deploy_sf_snyc_jobs.py
    - this script will install all of the jobs needed.
3) load_dp_accuracy_day_nz 
    - turn on the job with continuous running
    - set alerts to the groups that will watch this.
4) load_dp_forecast_week_hist_nz
    - turn on the job with continuous running
    - set alerts to the groups that will watch this.
5) load_dp_order_projection_day_hist_nz
    - turn on the job with continuous running
    - set alerts to the groups that will watch this.
6) load_dp_order_projection_week_hist_nz
    - turn on the job with continuous running
    - set alerts to the groups that will watch this.
7) load_dp_product_location_settings_hist_nz
    - turn on the job with continuous running
    - set alerts to the groups that will watch this.

**Jobs that are being deployed**

- load_dp_accuracy_day_nz.json 
- load_dp_forecast_week_hist_nz.json
- load_dp_order_projection_day_hist_nz.json
- load_dp_order_projection_week_hist_nz.json
- load_dp_product_location_settings_hist_nz.json
