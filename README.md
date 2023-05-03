
# Netezza Datalake Migration

This repo is used to host the converted code and assets from the Netezza To Databricks migration.

## Repo Structure

``` text

└── nz-databricks-migration
    ├── Bladebridge **All Bladebridge** artifacts go here
    │   ├── configs **All Bladebridge** converter configs go here 
    │   ├── informatica **All Informatica** xml export got here
    │   └── netezza **All Netezza** DDL exports heere
    ├── Datalake **Root for converted code**
    │   ├── Services **Each Informatica folder is mapped here**
    │   │   ├── deployment **Deployment Terraform scripts should be places here**
    │   │   ├── notebooks **All converted Notebooks go here**
    │   │   └── tables **All converted SQL Scripts go here**
    │   └── WMS **BA_WMS Folder in Informatica**
    │       ├── deployment
    │       ├── notebooks
    │       └── tables
    └── README.md
```
