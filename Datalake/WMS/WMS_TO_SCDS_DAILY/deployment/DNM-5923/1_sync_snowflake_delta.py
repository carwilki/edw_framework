
from Datalake.utils.DeltaLakeWriter import SparkDeltaLakeWriter
from Datalake.utils.genericUtilities import getSfCredentials

env = "prod"
table_list = ["refine.WM_E_ACT"
"refine.WM_E_ACT_ELM",
"refine.WM_E_ACT_ELM_CRIT",
"refine.WM_E_AUD_LOG",
"refine.WM_E_CRIT_VAL",
"refine.WM_E_ELM",
"refine.WM_E_ELM_CRIT",
"refine.WM_E_EMP_STAT_CODE",
"refine.WM_E_EVNT_SMRY_HDR",
"refine.WM_E_LABOR_TYPE_CODE",
"refine.WM_E_MSRMNT_RULE_CALC",
"refine.WM_E_MSRMNT_RULE_CONDITION",
"refine.WM_E_SHIFT",
"refine.WM_ILM_APPOINTMENT_OBJECTS",
"refine.WM_ILM_APPT_EQUIPMENTS",
"refine.WM_ITEM_FACILITY_SLOTTING",
"refine.WM_LABOR_ACTIVITY",
"refine.WM_LABOR_CRITERIA",
"refine.WM_LOCN_GRP",
"refine.WM_LPN_LOCK",
"refine.WM_PICK_LOCN_DTL",
"refine.WM_PICK_LOCN_HDR",
"refine.WM_PUTAWAY_LOCK",
"refine.WM_RACK_TYPE",
"refine.WM_RACK_TYPE_LEVEL",
"refine.WM_RESV_LOCN_HDR",
"refine.WM_SYS_CODE",
"refine.WM_TRAILER_CONTENTS",
"refine.WM_YARD_ZONE",
"refine.WM_YARD_ZONE_SLOT"
]

#table_list = [table for table in args.table_list.split(",")]
# table_list = json.dumps(table_list)

sfOptions = getSfCredentials(env)

for table in table_list:
    print(table)
    delta_schema = table.split(".")[0]
    tableName = table.split(".")[1]
    SparkDeltaLakeWriter(sfOptions, tableName, delta_schema).pull_data()
