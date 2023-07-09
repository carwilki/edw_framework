import argparse
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Pick_Locn_Hdr_Slotting_PRE import (
    m_WM_Pick_Locn_Hdr_Slotting_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Order_Line_Item_PRE import (
    m_WM_Order_Line_Item_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Purchase_Orders_PRE import (
    m_WM_Purchase_Orders_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Asn_Detail_PRE import (
    m_WM_Asn_Detail_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Crit_Val_PRE import (
    m_WM_E_Crit_Val_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Ilm_Appointment_Status_PRE import (
    m_WM_Ilm_Appointment_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Shift_PRE import m_WM_E_Shift_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Msrmnt_PRE import m_WM_E_Msrmnt_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Msrmnt_Rule_Calc_PRE import (
    m_WM_E_Msrmnt_Rule_Calc_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Pick_Locn_Dtl_Slotting_PRE import (
    m_WM_Pick_Locn_Dtl_Slotting_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Ilm_Appt_Equipments_PRE import (
    m_WM_Ilm_Appt_Equipments_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Asn_Status_PRE import (
    m_WM_Asn_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Purchase_Orders_Line_Item_PRE import (
    m_WM_Purchase_Orders_Line_Item_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Orders_PRE import m_WM_Orders_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Evnt_Smry_Hdr_PRE import (
    m_WM_E_Evnt_Smry_Hdr_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Item_Facility_Mapping_Wms_PRE import (
    m_WM_Item_Facility_Mapping_Wms_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Trailer_Type_PRE import (
    m_WM_Trailer_Type_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Shipment_PRE import m_WM_Shipment_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Putaway_Lock_PRE import (
    m_WM_Putaway_Lock_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Purchase_Orders_Line_Status_PRE import (
    m_WM_Purchase_Orders_Line_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Asn_PRE import m_WM_Asn_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Stop_PRE import m_WM_Stop_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Item_Wms_PRE import m_WM_Item_Wms_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Job_Function_PRE import (
    m_WM_E_Job_Function_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Outpt_Lpn_PRE import (
    m_WM_Outpt_Lpn_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Act_Elm_Crit_PRE import (
    m_WM_E_Act_Elm_Crit_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Purchase_Orders_Status_PRE import (
    m_WM_Purchase_Orders_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Outpt_Lpn_Detail_PRE import (
    m_WM_Outpt_Lpn_Detail_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Msrmnt_Rule_Condition_PRE import (
    m_WM_E_Msrmnt_Rule_Condition_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Task_Dtl_PRE import m_WM_Task_Dtl_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Asn_Detail_Status_PRE import (
    m_WM_Asn_Detail_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Ilm_Task_Status_PRE import (
    m_WM_Ilm_Task_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Act_Elm_PRE import (
    m_WM_E_Act_Elm_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Shipment_Status_PRE import (
    m_WM_Shipment_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Outpt_Orders_PRE import (
    m_WM_Outpt_Orders_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Act_PRE import m_WM_E_Act_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Rack_Type_PRE import (
    m_WM_Rack_Type_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Msrmnt_Rule_PRE import (
    m_WM_E_Msrmnt_Rule_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Emp_Stat_Code_PRE import (
    m_WM_E_Emp_Stat_Code_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Labor_Msg_Crit_PRE import (
    m_WM_Labor_Msg_Crit_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Locn_Hdr_PRE import m_WM_Locn_Hdr_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Pick_Locn_Hdr_PRE import (
    m_WM_Pick_Locn_Hdr_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Product_Class_PRE import (
    m_WM_Product_Class_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Facility_PRE import m_WM_Facility_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Rack_Type_Level_PRE import (
    m_WM_Rack_Type_Level_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_C_Leader_Audit_PRE import (
    m_WM_C_Leader_Audit_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Slot_Item_PRE import (
    m_WM_Slot_Item_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Labor_Msg_Dtl_Crit_PRE import (
    m_WM_Labor_Msg_Dtl_Crit_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Item_Facility_Slotting_PRE import (
    m_WM_Item_Facility_Slotting_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Task_Hdr_PRE import m_WM_Task_Hdr_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Wave_Parm_PRE import (
    m_WM_Wave_Parm_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Outpt_Order_Line_Item_PRE import (
    m_WM_Outpt_Order_Line_Item_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Lpn_Status_PRE import (
    m_WM_Lpn_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Lpn_Facility_Status_PRE import (
    m_WM_Lpn_Facility_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Pick_Locn_Dtl_PRE import (
    m_WM_Pick_Locn_Dtl_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Size_Uom_PRE import m_WM_Size_Uom_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Vend_Perf_Tran_PRE import (
    m_WM_Vend_Perf_Tran_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Yard_PRE import m_WM_Yard_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Order_Status_PRE import (
    m_WM_Order_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Resv_Locn_Hdr_PRE import (
    m_WM_Resv_Locn_Hdr_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Labor_Criteria_PRE import (
    m_WM_Labor_Criteria_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Lpn_PRE import m_WM_Lpn_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Labor_Msg_Dtl_PRE import (
    m_WM_Labor_Msg_Dtl_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Slot_Item_Score_PRE import (
    m_WM_Slot_Item_Score_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Ilm_Appointments_PRE import (
    m_WM_Ilm_Appointments_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Lpn_Audit_Results_PRE import (
    m_WM_Lpn_Audit_Results_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Item_Cbo_PRE import m_WM_Item_Cbo_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Labor_Msg_PRE import (
    m_WM_Labor_Msg_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Pix_Tran_PRE import m_WM_Pix_Tran_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Equipment_PRE import (
    m_WM_Equipment_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Ilm_Appointment_Type_PRE import (
    m_WM_Ilm_Appointment_Type_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Labor_Activity_PRE import (
    m_WM_Labor_Activity_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Sec_User_PRE import m_WM_Sec_User_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Standard_Uom_PRE import (
    m_WM_Standard_Uom_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Lpn_Detail_PRE import (
    m_WM_Lpn_Detail_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Item_Group_Wms_PRE import (
    m_WM_Item_Group_Wms_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Item_Package_Cbo_PRE import (
    m_WM_Item_Package_Cbo_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Trailer_Ref_PRE import (
    m_WM_Trailer_Ref_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Equipment_Instance_PRE import (
    m_WM_Equipment_Instance_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Labor_Tran_Dtl_Crit_PRE import (
    m_WM_Labor_Tran_Dtl_Crit_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_UN_Number_PRE import (
    m_WM_UN_Number_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Elm_PRE import m_WM_E_Elm_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Elm_Crit_PRE import (
    m_WM_E_Elm_Crit_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Stop_Status_PRE import (
    m_WM_Stop_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Labor_Type_Code_PRE import (
    m_WM_E_Labor_Type_Code_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Commodity_Code_PRE import (
    m_WM_Commodity_Code_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Locn_Grp_PRE import m_WM_Locn_Grp_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Yard_Zone_PRE import (
    m_WM_Yard_Zone_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Business_Partner_PRE import (
    m_WM_Business_Partner_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Sys_Code_PRE import m_WM_Sys_Code_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Do_Status_PRE import (
    m_WM_Do_Status_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_Yard_Zone_Slot_PRE import (
    m_Yard_Zone_Slot_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Ilm_Appointment_Objects_PRE import (
    m_WM_Ilm_Appointment_Objects_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_User_Profile_PRE import (
    m_WM_User_Profile_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Lpn_Lock_PRE import m_WM_Lpn_Lock_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Lpn_Type_PRE import m_WM_Lpn_Type_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Dock_Door_PRE import (
    m_WM_Dock_Door_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_C_TMS_Plan_PRE import (
    m_WM_C_TMS_Plan_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Picking_Short_Item_PRE import (
    m_WM_Picking_Short_Item_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Aud_Log_PRE import (
    m_WM_E_Aud_Log_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Ship_Via_PRE import m_WM_Ship_Via_PRE
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Lpn_Size_Type_PRE import (
    m_WM_Lpn_Size_Type_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Trailer_Contents_PRE import (
    m_WM_Trailer_Contents_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Carrier_Code_PRE import (
    m_WM_Carrier_Code_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Ilm_Yard_Activity_PRE import (
    m_WM_Ilm_Yard_Activity_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_E_Emp_Dtl_PRE import (
    m_WM_E_Emp_Dtl_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Trailer_Visit_Pre import (
    m_WM_Trailer_Visit_PRE,
)
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Trailer_Visit_Detail_Pre import (
    m_WM_Trailer_Visit_Detail_PRE,
)

from logging import getLogger, INFO

parser = argparse.ArgumentParser()
logger = getLogger()

parser.add_argument("DC_NBR", type=str, help="DC number")
parser.add_argument("env", type=str, help="Env Variable")
parser.add_argument("setNo", type=str, help="setNo Variable")
args = parser.parse_args()
dcnbr = args.DC_NBR
env = args.env
setNo = args.setNo

if dcnbr is None or dcnbr == "":
    raise ValueError("DC_NBR is not set")

if env is None or env == "":
    raise ValueError("env is not set")

if setNo is None or setNo == "":
    raise ValueError("setNo is not set")


####################################################################
# foreach mapping in maplet/worklet call the corresponding notebook
# that is created.
# main section #
####################################################################


set_P_Y_lst = [
    "WM_PICKING_SHORT_ITEM",
    "WM_PIX_TRAN",
    "WM_PRODUCT_CLASS",
    "WM_PURCHASE_ORDERS",
    "WM_PURCHASE_ORDERS_LINE_ITEM",
    "WM_PURCHASE_ORDERS_LINE_STATUS",
    "WM_PURCHASE_ORDERS_STATUS",
    "WM_PUTAWAY_LOCK",
    "WM_RACK_TYPE",
    "WM_RACK_TYPE_LEVEL",
    "WM_RESV_LOCN_HDR",
    "WM_SEC_USER",
    "WM_SHIP_VIA",
    "WM_SHIPMENT",
    "WM_SHIPMENT_STATUS",
    "WM_SIZE_UOM",
    "WM_SLOT_ITEM",
    "WM_SLOT_ITEM_SCORE",
    "WM_STANDARD_UOM",
    "WM_STOP",
    "WM_STOP_STATUS",
    "WM_SYS_CODE",
    "WM_TASK_DTL",
    "WM_TASK_HDR",
    "WM_TRAILER_CONTENTS",
    "WM_TRAILER_REF",
    "WM_TRAILER_TYPE",
    "WM_TRAILER_VISIT",
    "WM_TRAILER_VISIT_DTL",
    "WM_UN_NUMBER",
    "WM_USER_PROFILE",
    "WM_VEND_PERF_TRAN",
    "WM_WAVE_PARM",
    "WM_YARD",
    "WM_YARD_ZONE",
    "WM_YARD_ZONE_SLOT",
]

set_I_P_lst = [
    "WM_ILM_TASK_STATUS",
    "WM_ILM_YARD_ACTIVITY",
    "WM_ITEM_CBO",
    "WM_ITEM_FACILITY_MAPPING_WMS",
    "WM_ITEM_FACILITY_SLOTTING",
    "WM_ITEM_GROUP_WMS",
    "WM_ITEM_PACKAGE_CBO",
    "WM_ITEM_WMS",
    "WM_LABOR_ACTIVITY",
    "WM_LABOR_CRITERIA",
    "WM_LABOR_MSG",
    "WM_LABOR_MSG_CRIT",
    "WM_LABOR_MSG_DTL",
    "WM_LABOR_MSG_DTL_CRIT",
    "WM_LABOR_TRAN_DTL_CRIT",
    "WM_LOCN_GRP",
    "WM_LOCN_HDR",
    "WM_LPN",
    "WM_LPN_AUDIT_RESULTS",
    "WM_LPN_DETAIL",
    "WM_LPN_FACILITY_STATUS",
    "WM_LPN_LOCK",
    "WM_LPN_SIZE_TYPE",
    "WM_LPN_STATUS",
    "WM_LPN_TYPE",
    "WM_ORDER_LINE_ITEM",
    "WM_ORDER_STATUS",
    "WM_ORDERS",
    "WM_OUTPT_LPN",
    "WM_OUTPT_LPN_DETAIL",
    "WM_OUTPT_ORDER_LINE_ITEM",
    "WM_OUTPT_ORDERS",
    "WM_PICK_LOCN_DTL",
    "WM_PICK_LOCN_DTL_SLOTTING",
    "WM_PICK_LOCN_HDR",
    "WM_PICK_LOCN_HDR_SLOTTING",
]

set_A_I_lst = [
    "WM_ASN",
    "WM_ASN_DETAIL",
    "WM_ASN_DETAIL_STATUS",
    "WM_ASN_STATUS",
    "WM_BUSINESS_PARTNER",
    "WM_C_LEADER_AUDIT",
    "WM_C_TMS_PLAN",
    "WM_CARRIER_CODE",
    "WM_COMMODITY_CODE",
    "WM_DO_STATUS",
    "WM_DOCK_DOOR",
    "WM_E_ACT",
    "WM_E_ACT_ELM",
    "WM_E_ACT_ELM_CRIT",
    "WM_E_AUD_LOG",
    "WM_E_CRIT_VAL",
    "WM_E_ELM",
    "WM_E_ELM_CRIT",
    "WM_E_EMP_DTL",
    "WM_E_EMP_STAT_CODE",
    "WM_E_EVNT_SMRY_HDR",
    "WM_E_JOB_FUNCTION",
    "WM_E_LABOR_TYPE_CODE",
    "WM_E_MSRMNT",
    "WM_E_MSRMNT_RULE",
    "WM_E_MSRMNT_RULE_CALC",
    "WM_E_MSRMNT_RULE_CONDITION",
    "WM_E_SHIFT",
    "WM_EQUIPMENT",
    "WM_EQUIPMENT_INSTANCE",
    "WM_FACILITY",
    "WM_ILM_APPOINTMENT_OBJECTS",
    "WM_ILM_APPOINTMENT_STATUS",
    "WM_ILM_APPOINTMENT_TYPE",
    "WM_ILM_APPOINTMENTS",
    "WM_ILM_APPT_EQUIPMENTS",
]


def tableGroupsNfunc(dcnbr, env, setNo):
    if setNo == "set_A_I_1":
        for table in set_A_I_lst[:18]:
            preTable_func = "m_".join(table).join("_PRE")
            preTable_func(dcnbr, env)
            logger.info(f"{0} executed".format(preTable_func))
    elif setNo == "set_A_I_2":
        for table in set_A_I_lst[18:36]:
            preTable_func = "m_".join(table).join("_PRE")
            preTable_func(dcnbr, env)
            logger.info(f"{0} executed".format(preTable_func))
    elif setNo == "set_I_P_1":
        for table in set_I_P_lst[:18]:
            preTable_func = "m_".join(table).join("_PRE")
            preTable_func(dcnbr, env)
            logger.info(f"{0} executed".format(preTable_func))
    elif setNo == "set_I_P_2":
        for table in set_I_P_lst[18:36]:
            preTable_func = "m_".join(table).join("_PRE")
            preTable_func(dcnbr, env)
            logger.info(f"{0} executed".format(preTable_func))
    elif setNo == "set_P_Y_1":
        for table in set_I_P_lst[:18]:
            preTable_func = "m_".join(table).join("_PRE")
            preTable_func(dcnbr, env)
            logger.info(f"{0} executed".format(preTable_func))
    else:
        for table in set_I_P_lst[18:36]:
            preTable_func = "m_".join(table).join("_PRE")
            preTable_func(dcnbr, env)
            logger.info(f"{0} executed".format(preTable_func))


tableGroupsNfunc(dcnbr, env, setNo)


""" m_WM_Pick_Locn_Hdr_Slotting_PRE(dcnbr, env)
logger.info("m_WM_Pick_Locn_Hdr_Slotting_PRE executed")
m_WM_Order_Line_Item_PRE(dcnbr, env)
logger.info("m_WM_Order_Line_Item_PRE executed")
m_WM_Purchase_Orders_PRE(dcnbr, env)
logger.info("m_WM_Purchase_Orders_PRE executed")
m_WM_Asn_Detail_PRE(dcnbr, env)
logger.info("m_WM_Asn_Detail_PRE executed")
m_WM_E_Crit_Val_PRE(dcnbr, env)
logger.info("m_WM_E_Crit_Val_PRE executed")
m_WM_Ilm_Appointment_Status_PRE(dcnbr, env)
logger.info("m_WM_Ilm_Appointment_Status_PRE executed")
m_WM_E_Shift_PRE(dcnbr, env)
logger.info("m_WM_E_Shift_PRE executed")
m_WM_E_Msrmnt_PRE(dcnbr, env)
logger.info("m_WM_E_Msrmnt_PRE executed")
m_WM_E_Msrmnt_Rule_Calc_PRE(dcnbr, env)
logger.info("m_WM_E_Msrmnt_Rule_Calc_PRE executed")
m_WM_Pick_Locn_Dtl_Slotting_PRE(dcnbr, env)
logger.info("m_WM_Pick_Locn_Dtl_Slotting_PRE executed")
m_WM_Ilm_Appt_Equipments_PRE(dcnbr, env)
logger.info("m_WM_Ilm_Appt_Equipments_PRE executed")
m_WM_Asn_Status_PRE(dcnbr, env)
logger.info("m_WM_Asn_Status_PRE executed")
m_WM_Purchase_Orders_Line_Item_PRE(dcnbr, env)
logger.info("m_WM_Purchase_Orders_Line_Item_PRE executed")
m_WM_Orders_PRE(dcnbr, env)
logger.info("m_WM_Orders_PRE executed")
m_WM_E_Evnt_Smry_Hdr_PRE(dcnbr, env)
logger.info("m_WM_E_Evnt_Smry_Hdr_PRE executed")
m_WM_Item_Facility_Mapping_Wms_PRE(dcnbr, env)
logger.info("m_WM_Item_Facility_Mapping_Wms_PRE executed")
m_WM_Trailer_Type_PRE(dcnbr, env)
logger.info("m_WM_Trailer_Type_PRE executed")
m_WM_Shipment_PRE(dcnbr, env)
logger.info("m_WM_Shipment_PRE executed")
m_WM_Putaway_Lock_PRE(dcnbr, env)
logger.info("m_WM_Putaway_Lock_PRE executed")
m_WM_Purchase_Orders_Line_Status_PRE(dcnbr, env)
logger.info("m_WM_Purchase_Orders_Line_Status_PRE executed")
m_WM_Asn_PRE(dcnbr, env)
logger.info("m_WM_Asn_PRE executed")
m_WM_Stop_PRE(dcnbr, env)
logger.info("m_WM_Stop_PRE executed")
m_WM_Item_Wms_PRE(dcnbr, env)
logger.info("m_WM_Item_Wms_PRE executed")
m_WM_E_Job_Function_PRE(dcnbr, env)
logger.info("m_WM_E_Job_Function_PRE executed")
m_WM_Outpt_Lpn_PRE(dcnbr, env)
logger.info("m_WM_Outpt_Lpn_PRE executed")
m_WM_E_Act_Elm_Crit_PRE(dcnbr, env)
logger.info("m_WM_E_Act_Elm_Crit_PRE executed")
m_WM_Purchase_Orders_Status_PRE(dcnbr, env)
logger.info("m_WM_Purchase_Orders_Status_PRE executed")
m_WM_Outpt_Lpn_Detail_PRE(dcnbr, env)
logger.info("m_WM_Outpt_Lpn_Detail_PRE executed")
m_WM_E_Msrmnt_Rule_Condition_PRE(dcnbr, env)
logger.info("m_WM_E_Msrmnt_Rule_Condition_PRE executed")
m_WM_Task_Dtl_PRE(dcnbr, env)
logger.info("m_WM_Task_Dtl_PRE executed")
m_WM_Asn_Detail_Status_PRE(dcnbr, env)
logger.info("m_WM_Asn_Detail_Status_PRE executed")
m_WM_Ilm_Task_Status_PRE(dcnbr, env)
logger.info("m_WM_Ilm_Task_Status_PRE executed")
m_WM_E_Act_Elm_PRE(dcnbr, env)
logger.info("m_WM_E_Act_Elm_PRE executed")
m_WM_Shipment_Status_PRE(dcnbr, env)
logger.info("m_WM_Shipment_Status_PRE executed")
m_WM_Outpt_Orders_PRE(dcnbr, env)
logger.info("m_WM_Outpt_Orders_PRE executed")
m_WM_E_Act_PRE(dcnbr, env)
logger.info("m_WM_E_Act_PRE executed")
m_WM_Rack_Type_PRE(dcnbr, env)
logger.info("m_WM_Rack_Type_PRE executed")
m_WM_E_Msrmnt_Rule_PRE(dcnbr, env)
logger.info("m_WM_E_Msrmnt_Rule_PRE executed")
m_WM_E_Emp_Stat_Code_PRE(dcnbr, env)
logger.info("m_WM_E_Emp_Stat_Code_PRE executed")
m_WM_Labor_Msg_Crit_PRE(dcnbr, env)
logger.info("m_WM_Labor_Msg_Crit_PRE executed")
m_WM_Locn_Hdr_PRE(dcnbr, env)
logger.info("m_WM_Locn_Hdr_PRE executed")
m_WM_Pick_Locn_Hdr_PRE(dcnbr, env)
logger.info("m_WM_Pick_Locn_Hdr_PRE executed")
m_WM_Product_Class_PRE(dcnbr, env)
logger.info("m_WM_Product_Class_PRE executed")
m_WM_Facility_PRE(dcnbr, env)
logger.info("m_WM_Facility_PRE executed")
m_WM_Rack_Type_Level_PRE(dcnbr, env)
logger.info("m_WM_Rack_Type_Level_PRE executed")
m_WM_C_Leader_Audit_PRE(dcnbr, env)
logger.info("m_WM_C_Leader_Audit_PRE executed")
m_WM_Slot_Item_PRE(dcnbr, env)
logger.info("m_WM_Slot_Item_PRE executed")
m_WM_Labor_Msg_Dtl_Crit_PRE(dcnbr, env)
logger.info("m_WM_Labor_Msg_Dtl_Crit_PRE executed")
m_WM_Item_Facility_Slotting_PRE(dcnbr, env)
logger.info("m_WM_Item_Facility_Slotting_PRE executed")
m_WM_Task_Hdr_PRE(dcnbr, env)
logger.info("m_WM_Task_Hdr_PRE executed")
m_WM_Wave_Parm_PRE(dcnbr, env)
logger.info("m_WM_Wave_Parm_PRE executed")
m_WM_Outpt_Order_Line_Item_PRE(dcnbr, env)
logger.info("m_WM_Outpt_Order_Line_Item_PRE executed")
m_WM_Lpn_Status_PRE(dcnbr, env)
logger.info("m_WM_Lpn_Status_PRE executed")
m_WM_Lpn_Facility_Status_PRE(dcnbr, env)
logger.info("m_WM_Lpn_Facility_Status_PRE executed")
m_WM_Pick_Locn_Dtl_PRE(dcnbr, env)
logger.info("m_WM_Pick_Locn_Dtl_PRE executed")
m_WM_Size_Uom_PRE(dcnbr, env)
logger.info("m_WM_Size_Uom_PRE executed")
m_WM_Vend_Perf_Tran_PRE(dcnbr, env)
logger.info("m_WM_Vend_Perf_Tran_PRE executed")
m_WM_Yard_PRE(dcnbr, env)
logger.info("m_WM_Yard_PRE executed")
m_WM_Order_Status_PRE(dcnbr, env)
logger.info("m_WM_Order_Status_PRE executed")
m_WM_Resv_Locn_Hdr_PRE(dcnbr, env)
logger.info("m_WM_Resv_Locn_Hdr_PRE executed")
m_WM_Labor_Criteria_PRE(dcnbr, env)
logger.info("m_WM_Labor_Criteria_PRE executed")
m_WM_Lpn_PRE(dcnbr, env)
logger.info("m_WM_Lpn_PRE executed")
m_WM_Labor_Msg_Dtl_PRE(dcnbr, env)
logger.info("m_WM_Labor_Msg_Dtl_PRE executed")
m_WM_Slot_Item_Score_PRE(dcnbr, env)
logger.info("m_WM_Slot_Item_Score_PRE executed")
m_WM_Ilm_Appointments_PRE(dcnbr, env)
logger.info("m_WM_Ilm_Appointments_PRE executed")
m_WM_Lpn_Audit_Results_PRE(dcnbr, env)
logger.info("m_WM_Lpn_Audit_Results_PRE executed")
m_WM_Item_Cbo_PRE(dcnbr, env)
logger.info("m_WM_Item_Cbo_PRE executed")
m_WM_Labor_Msg_PRE(dcnbr, env)
logger.info("m_WM_Labor_Msg_PRE executed")
m_WM_Pix_Tran_PRE(dcnbr, env)
logger.info("m_WM_Pix_Tran_PRE executed")
m_WM_Equipment_PRE(dcnbr, env)
logger.info("m_WM_Equipment_PRE executed")
m_WM_Ilm_Appointment_Type_PRE(dcnbr, env)
logger.info("m_WM_Ilm_Appointment_Type_PRE executed")
m_WM_Labor_Activity_PRE(dcnbr, env)
logger.info("m_WM_Labor_Activity_PRE executed")
m_WM_Sec_User_PRE(dcnbr, env)
logger.info("m_WM_Sec_User_PRE executed")
m_WM_Standard_Uom_PRE(dcnbr, env)
logger.info("m_WM_Standard_Uom_PRE executed")
m_WM_Lpn_Detail_PRE(dcnbr, env)
logger.info("m_WM_Lpn_Detail_PRE executed")
m_WM_Item_Group_Wms_PRE(dcnbr, env)
logger.info("m_WM_Item_Group_Wms_PRE executed")
m_WM_Item_Package_Cbo_PRE(dcnbr, env)
logger.info("m_WM_Item_Package_Cbo_PRE executed")
m_WM_Trailer_Ref_PRE(dcnbr, env)
logger.info("m_WM_Trailer_Ref_PRE executed")
m_WM_Equipment_Instance_PRE(dcnbr, env)
logger.info("m_WM_Equipment_Instance_PRE executed")
m_WM_Labor_Tran_Dtl_Crit_PRE(dcnbr, env)
logger.info("m_WM_Labor_Tran_Dtl_Crit_PRE executed")
m_WM_UN_Number_PRE(dcnbr, env)
logger.info("m_WM_UN_Number_PRE executed")
m_WM_E_Elm_PRE(dcnbr, env)
logger.info("m_WM_E_Elm_PRE executed")
m_WM_E_Elm_Crit_PRE(dcnbr, env)
logger.info("m_WM_E_Elm_Crit_PRE executed")
m_WM_Stop_Status_PRE(dcnbr, env)
logger.info("m_WM_Stop_Status_PRE executed")
m_WM_E_Labor_Type_Code_PRE(dcnbr, env)
logger.info("m_WM_E_Labor_Type_Code_PRE executed")
m_WM_Commodity_Code_PRE(dcnbr, env)
logger.info("m_WM_Commodity_Code_PRE executed")
m_WM_Locn_Grp_PRE(dcnbr, env)
logger.info("m_WM_Locn_Grp_PRE executed")
m_WM_Yard_Zone_PRE(dcnbr, env)
logger.info("m_WM_Yard_Zone_PRE executed")
m_WM_Business_Partner_PRE(dcnbr, env)
logger.info("m_WM_Business_Partner_PRE executed")
m_WM_Sys_Code_PRE(dcnbr, env)
logger.info("m_WM_Sys_Code_PRE executed")
m_WM_Do_Status_PRE(dcnbr, env)
logger.info("m_WM_Do_Status_PRE executed")
m_Yard_Zone_Slot_PRE(dcnbr, env)
logger.info("m_Yard_Zone_Slot_PRE executed")
m_WM_Ilm_Appointment_Objects_PRE(dcnbr, env)
logger.info("m_WM_Ilm_Appointment_Objects_PRE executed")
m_WM_User_Profile_PRE(dcnbr, env)
logger.info("m_WM_User_Profile_PRE executed")
m_WM_Lpn_Lock_PRE(dcnbr, env)
logger.info("m_WM_Lpn_Lock_PRE executed")
m_WM_Lpn_Type_PRE(dcnbr, env)
logger.info("m_WM_Lpn_Type_PRE executed")
m_WM_Dock_Door_PRE(dcnbr, env)
logger.info("m_WM_Dock_Door_PRE executed")
m_WM_C_TMS_Plan_PRE(dcnbr, env)
logger.info("m_WM_C_TMS_Plan_PRE executed")
m_WM_Picking_Short_Item_PRE(dcnbr, env)
logger.info("m_WM_Picking_Short_Item_PRE executed")
m_WM_E_Aud_Log_PRE(dcnbr, env)
logger.info("m_WM_E_Aud_Log_PRE executed")
m_WM_Ship_Via_PRE(dcnbr, env)
logger.info("m_WM_Ship_Via_PRE executed")
m_WM_Lpn_Size_Type_PRE(dcnbr, env)
logger.info("m_WM_Lpn_Size_Type_PRE executed")
m_WM_Trailer_Contents_PRE(dcnbr, env)
logger.info("m_WM_Trailer_Contents_PRE executed")
m_WM_Carrier_Code_PRE(dcnbr, env)
logger.info("m_WM_Carrier_Code_PRE executed")
m_WM_Ilm_Yard_Activity_PRE(dcnbr, env)
logger.info("m_WM_Ilm_Yard_Activity_PRE executed")
m_WM_E_Emp_Dtl_PRE(dcnbr, env)
logger.info("m_WM_E_Emp_Dtl_PRE executed")
m_WM_Trailer_Visit_Pre(dcnbr, env)
logger.info("m_WM_Trailer_Visit_Pre executed")
m_WM_Trailer_Visit_Detail_Pre(dcnbr, env)
logger.info("m_WM_Trailer_Visit_Detail_Pre executed")
 """
