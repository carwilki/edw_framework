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
    m_WM_Picking_Short_Item_PRE,
    m_WM_Pix_Tran_PRE,
    m_WM_Product_Class_PRE,
    m_WM_Purchase_Orders_PRE,
    m_WM_Purchase_Orders_Line_Item_PRE,
    m_WM_Purchase_Orders_Line_Status_PRE,
    m_WM_Purchase_Orders_Status_PRE,
    m_WM_Putaway_Lock_PRE,
    m_WM_Rack_Type_PRE,
    m_WM_Rack_Type_Level_PRE,
    m_WM_Resv_Locn_Hdr_PRE,
    m_WM_Sec_User_PRE,
    m_WM_Ship_Via_PRE,
    m_WM_Shipment_PRE,
    m_WM_Shipment_Status_PRE,
    m_WM_Size_Uom_PRE,
    m_WM_Slot_Item_PRE,
    m_WM_Slot_Item_Score_PRE,
    m_WM_Standard_Uom_PRE,
    m_WM_Stop_PRE,
    m_WM_Stop_Status_PRE,
    m_WM_Sys_Code_PRE,
    m_WM_Task_Dtl_PRE,
    m_WM_Task_Hdr_PRE,
    m_WM_Trailer_Contents_PRE,
    m_WM_Trailer_Ref_PRE,
    m_WM_Trailer_Type_PRE,
    m_WM_Trailer_Visit_PRE,
    m_WM_Trailer_Visit_Detail_PRE,
    m_WM_UN_Number_PRE,
    m_WM_User_Profile_PRE,
    m_WM_Vend_Perf_Tran_PRE,
    m_WM_Wave_Parm_PRE,
    m_WM_Yard_PRE,
    m_WM_Yard_Zone_PRE,
    m_Yard_Zone_Slot_PRE,
]

set_I_P_lst = [
    m_WM_Ilm_Task_Status_PRE,
    m_WM_Ilm_Yard_Activity_PRE,
    m_WM_Item_Cbo_PRE,
    m_WM_Item_Facility_Mapping_Wms_PRE,
    m_WM_Item_Facility_Slotting_PRE,
    m_WM_Item_Group_Wms_PRE,
    m_WM_Item_Package_Cbo_PRE,
    m_WM_Item_Wms_PRE,
    m_WM_Labor_Activity_PRE,
    m_WM_Labor_Criteria_PRE,
    m_WM_Labor_Msg_PRE,
    m_WM_Labor_Msg_Crit_PRE,
    m_WM_Labor_Msg_Dtl_PRE,
    m_WM_Labor_Msg_Dtl_Crit_PRE,
    m_WM_Labor_Tran_Dtl_Crit_PRE,
    m_WM_Locn_Grp_PRE,
    m_WM_Locn_Hdr_PRE,
    m_WM_Lpn_PRE,
    m_WM_Lpn_Audit_Results_PRE,
    m_WM_Lpn_Detail_PRE,
    m_WM_Lpn_Facility_Status_PRE,
    m_WM_Lpn_Lock_PRE,
    m_WM_Lpn_Size_Type_PRE,
    m_WM_Lpn_Status_PRE,
    m_WM_Lpn_Type_PRE,
    m_WM_Order_Line_Item_PRE,
    m_WM_Order_Status_PRE,
    m_WM_Orders_PRE,
    m_WM_Outpt_Lpn_PRE,
    m_WM_Outpt_Lpn_Detail_PRE,
    m_WM_Outpt_Order_Line_Item_PRE,
    m_WM_Outpt_Orders_PRE,
    m_WM_Pick_Locn_Dtl_PRE,
    m_WM_Pick_Locn_Dtl_Slotting_PRE,
    m_WM_Pick_Locn_Hdr_PRE,
    m_WM_Pick_Locn_Hdr_Slotting_PRE,
]

set_A_I_lst = [
    m_WM_Asn_PRE,
    m_WM_Asn_Detail_PRE,
    m_WM_Asn_Detail_Status_PRE,
    m_WM_Asn_Status_PRE,
    m_WM_Business_Partner_PRE,
    m_WM_C_Leader_Audit_PRE,
    m_WM_C_TMS_Plan_PRE,
    m_WM_Carrier_Code_PRE,
    m_WM_Commodity_Code_PRE,
    m_WM_Do_Status_PRE,
    m_WM_Dock_Door_PRE,
    m_WM_E_Act_PRE,
    m_WM_E_Act_Elm_PRE,
    m_WM_E_Act_Elm_Crit_PRE,
    m_WM_E_Aud_Log_PRE,
    m_WM_E_Crit_Val_PRE,
    m_WM_E_Elm_PRE,
    m_WM_E_Elm_Crit_PRE,
    m_WM_E_Emp_Dtl_PRE,
    m_WM_E_Emp_Stat_Code_PRE,
    m_WM_E_Evnt_Smry_Hdr_PRE,
    m_WM_E_Job_Function_PRE,
    m_WM_E_Labor_Type_Code_PRE,
    m_WM_E_Msrmnt_PRE,
    m_WM_E_Msrmnt_Rule_PRE,
    m_WM_E_Msrmnt_Rule_Calc_PRE,
    m_WM_E_Msrmnt_Rule_Condition_PRE,
    m_WM_E_Shift_PRE,
    m_WM_Equipment_PRE,
    m_WM_Equipment_Instance_PRE,
    m_WM_Facility_PRE,
    m_WM_Ilm_Appointment_Objects_PRE,
    m_WM_Ilm_Appointment_Status_PRE,
    m_WM_Ilm_Appointment_Type_PRE,
    m_WM_Ilm_Appointments_PRE,
    m_WM_Ilm_Appt_Equipments_PRE,
]


def tableGroupsNfunc(dcnbr, env, setNo):
    if setNo == "set_A_I_1":
        for table in set_A_I_lst[:18]:
            # preTable_method_name = "m_".join(table).join("_PRE")
            table(dcnbr, env)
            logger.info(f"{0} executed".format(str(table)))
    elif setNo == "set_A_I_2":
        for table in set_A_I_lst[18:36]:
            # preTable_method_name = "m_".join(table).join("_PRE")
            table(dcnbr, env)
            logger.info(f"{0} executed".format(str(table)))
    elif setNo == "set_I_P_1":
        for table in set_I_P_lst[:18]:
            # preTable_method_name = "m_".join(table).join("_PRE")
            table(dcnbr, env)
            logger.info(f"{0} executed".format(str(table)))
    elif setNo == "set_I_P_2":
        for table in set_I_P_lst[18:36]:
            # preTable_method_name = "m_".join(table).join("_PRE")
            table(dcnbr, env)
            logger.info(f"{0} executed".format(str(table)))
    elif setNo == "set_P_Y_1":
        for table in set_P_Y_lst[:18]:
            # preTable_method_name = "m_".join(table).join("_PRE")
            table(dcnbr, env)
            logger.info(f"{0} executed".format(str(table)))
    elif setNo == "set_P_Y_2":
        for table in set_P_Y_lst[18:36]:
            # preTable_method_name = "m_".join(table).join("_PRE")
            table(dcnbr, env)
            logger.info(f"{0} executed".format(str(table)))            
    else:
        for table in set_I_P_lst[18:36]:
            # preTable_method_name = "m_".join(table).join("_PRE")
            table(dcnbr, env)
            logger.info(f"{0} executed".format(str(table)))


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
