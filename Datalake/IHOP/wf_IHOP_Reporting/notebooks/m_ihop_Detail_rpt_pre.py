# Databricks notebook source
# Code converted on 2023-08-24 13:53:58
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username,password,connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_AllocationEstimatedTrucks, type SOURCE 
# COLUMN COUNT: 67

SQ_Shortcut_to_AllocationEstimatedTrucks = jdbcSqlServerConnection(f"""(SELECT 

A.Allocationid,

A.AllocationQuantity,

A.CapacityWaveEligible,

A.DistributionCenterId DcId,

A.EventId,

A.SKU,

A.StoreId,

A.VendorId,

A.WaveTypeId,

AWA.AllocationWaveAssignmentId,

AWA.PrimaryWaveId,

AWA.NeedByDate,

Art.Name ArticleName,

Art.CubicVolume,

Art.CaseQuantity,

Art.SlottingGroupId,

Art.CubicVolumeByPieceAndNotCase,

CT.ConstraintTypeId,

CT.Code CTypeCode,

CT.Name CTypeName,

CT.Description CTypeDesc,

CT.IsStoreDefault,

CT.IsWaveEligible,

DC.Name DcName,

DC.FlaggedForReview,

DC.IsDeleted DcIsDeleted,

DCTS.LeadTimeInDays DLeadTimeInDays,

(ISNULL(DCTSPD.PickDayOfWeek,0)+ ISNULL(DCTS.LeadTimeInDays,0)) Expected_Delivery_Dt,

DCTSPD.PickDayOfWeek,

DCTSPD.ReplenishmentDayOfWeek,

E.Name EventName,

E.StartDate,

E.EndDate,

E.Description DescEvent,

E.EventParameterId,

OAA.DeliveryDate OB_Alloc_Assig_Delivery_Dt,

OAA.Quantity OB_Alloc_Assig_Qty,

S.Description StoreDesc,

SG.Name SlottingGroupName,

STO.StoreHoldOutId,

STO.SplitOrder,

STO.PurchasingGroup,

STO.DeliveryDate ReplenDate,

STO.Quantity Store_Trans_Orders,

VEP.VendorToDcSmoothingTypeId VenToDcSmoothTypeId,

VEP.IncrementalLeadTimeAboveStandard Vend_Holiday_Lead_Time,

VEP.VendorCapacity,

VEP.TruckReleaseDate,

VEP.DistributionCenterHoldOutDate DcHoldOutDate,

V.Name VendorName,

V.IsDeleted VendorIsDeleted,

V.StandardLeadTime StdLeadTime,

VTDC.LeadTimeInDays VLeadTimeInDays,

VTDCST.Code VendToDcSmootingTypeCode,

VTDCST.Name VendToDcSmootingTypeName,

VTDCST.Description VendToDcSmootingTypeDesc,

W.WaveId,

W.Code WaveCode,

W.ReleaseDate,

W.IsPrimary,

W.IsVendorManaged,

CASE WHEN NOT (W.IsVendorManaged = 1 AND VTDCST.Code = 200) THEN 'Y' ELSE 'N' END PetSmartManaged,

CASE WHEN W.IsVendorManaged = 1 AND VTDCST.Code = 200 THEN 'Y' ELSE 'N' END VendorManaged,

WT.Code WaveTypeCode,

WT.Name WaveTypeName,

Art.CubicVolume CaseCube,

(CASE WHEN Art.CubicVolumeByPieceAndNotCase = 1 THEN

      cast(OAA.Quantity as float) * Art.CubicVolume

ELSE

      (cast(OAA.Quantity as float) /cast(Art.CaseQuantity as float)) * Art.CubicVolume

END 

) WaveCube



FROM HolidayPlanning.dbo.Allocations A

FULL OUTER JOIN HolidayPlanning.dbo.AllocationWaveAssignments AWA on AWA.AllocationId = A.AllocationId

FULL OUTER JOIN HolidayPlanning.dbo.Events E ON E.EventId = A.EventId

FULL OUTER JOIN (SELECT OAA.*, W.EventId FROM HolidayPlanning.dbo.OutboundAllocationAssignments OAA

                        INNER JOIN HolidayPlanning.dbo.Waves W ON oaa.WaveId = w.WaveId) OAA ON 

            (OAA.EventId = A.EventId 

            AND OAA.StoreId = A.StoreId 

            AND OAA.SKU = A.SKU)

FULL OUTER JOIN HolidayPlanning.dbo.Waves W ON 

            (W.WaveId = AWA.PrimaryWaveId

            AND W.EventId = A.EventId)

FULL OUTER JOIN HolidayPlanning.dbo.WaveTypes WT ON WT.WaveTypeId = W.WaveTypeId

FULL OUTER JOIN HolidayPlanning.dbo.DistributionCenters DC ON DC.DistributionCenterId = A.DistributionCenterId

FULL OUTER JOIN HolidayPlanning.dbo.Stores S ON S.StoreID = A.StoreID

FULL OUTER JOIN HolidayPlanning.dbo.ConstraintTypes CT ON CT.ConstraintTypeId = S.ConstraintTypeId



--FULL OUTER JOIN HolidayPlanning.dbo.Articles Art ON Art.SKU = A.SKU



FULL OUTER JOIN ( select * from HolidayPlanning.dbo.Articles where casequantity <> 0 ) Art ON Art.SKU = A.SKU



FULL OUTER JOIN HolidayPlanning.dbo.SlottingGroups SG ON SG.SlottingGroupId = Art.SlottingGroupId

FULL OUTER JOIN HolidayPlanning.dbo.Vendors V ON V.VendorID = A.VendorID

FULL OUTER JOIN HolidayPlanning.dbo.VendorEventParameters VEP ON 

            (VEP.EventId = E.EventId 

            AND VEP.VendorId = V.VendorId)

FULL OUTER JOIN HolidayPlanning.dbo.VendorToDcSmoothingTypes VTDCST ON VTDCST.VendorToDcSmoothingTypeId = VEP.VendorToDcSmoothingTypeId

FULL OUTER JOIN HolidayPlanning.dbo.WaveGroupNeedByDates WGND ON WGND.PrimaryWaveId = AWA.PrimaryWaveId

FULL OUTER JOIN HolidayPlanning.dbo.DistributionCentersToStores DCTS ON

        (DCTS.DistributionCenterId = A.DistributionCenterId 

            AND DCTS.StoreId = A.StoreId )

FULL OUTER JOIN HolidayPlanning.dbo.StoreTransferOrders STO ON

        (STO.DistributionCenterId = A.DistributionCenterId 

        AND STO.StoreID = A.StoreId 

            AND STO.SKU = A.SKU)

FULL OUTER JOIN HolidayPlanning.dbo.VendorsToDistributionCenters VTDC ON

        (VTDC.DistributionCenterId = A.DistributionCenterId 

            AND VTDC.VendorId = A.VendorId)

FULL OUTER JOIN HolidayPlanning.dbo.DistributionCentersToStoresPickDays DCTSPD ON

       (DCTSPD.EventId = A.EventId 

            AND DCTSPD.DistributionCenterId = A.DistributionCenterId 

        AND DCTSPD.StoreId = A.StoreId 

        AND DCTSPD.PickDayOfWeek = DATEPART(dw,OAA.DeliveryDate))) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_AllocationEstimatedTrucks = SQ_Shortcut_to_AllocationEstimatedTrucks \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[0],'Allocationid') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[1],'AllocationQuantity') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[2],'CapacityWaveEligible') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[3],'DcId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[4],'EventId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[5],'SKU') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[6],'StoreId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[7],'VendorId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[8],'WaveTypeId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[9],'AllocationWaveAssignmentId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[10],'PrimaryWaveId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[11],'NeedByDate') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[12],'ArticleName') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[13],'CubicVolume') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[14],'CaseQuantity') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[15],'SlottingGroupId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[16],'CubicVolumeByPieceAndNotCase') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[17],'ConstraintTypeId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[18],'CTypeCode') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[19],'CTypeName') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[20],'CTypeDesc') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[21],'IsStoreDefault') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[22],'IsWaveEligible') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[23],'DcName') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[24],'FlaggedForReview') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[25],'DcIsDeleted') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[26],'DLeadTimeInDays') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[27],'Expected_Delivery_Dt') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[28],'PickDayOfWeek') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[29],'ReplenishmentDayOfWeek') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[30],'EventName') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[31],'StartDate') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[32],'EndDate') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[33],'DescEvent') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[34],'EventParameterId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[35],'OB_Alloc_Assig_Delivery_Dt') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[36],'OB_Alloc_Assig_Qty') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[37],'StoreDesc') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[38],'SlottingGroupName') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[39],'StoreHoldOutId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[40],'SplitOrder') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[41],'PurchasingGroup') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[42],'ReplenDate') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[43],'Store_Trans_Orders') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[44],'VenToDcSmoothTypeId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[45],'Vend_Holiday_Lead_Time') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[46],'VendorCapacity') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[47],'TruckReleaseDate') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[48],'DcHoldOutDate') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[49],'VendorName') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[50],'VendorIsDeleted') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[51],'StdLeadTime') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[52],'VLeadTimeInDays') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[53],'VendToDcSmootingTypeCode') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[54],'VendToDcSmootingTypeName') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[55],'VendToDcSmootingTypeDesc') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[56],'WaveId') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[57],'WaveCode') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[58],'ReleaseDate') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[59],'IsPrimary') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[60],'IsVendorManaged') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[61],'PetSmartManaged') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[62],'VendorManaged') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[63],'WaveTypeCode') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[64],'WaveTypeName') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[65],'CaseCube') \
	.withColumnRenamed(SQ_Shortcut_to_AllocationEstimatedTrucks.columns[66],'WaveCube')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column

EXPTRANS = SQ_Shortcut_to_AllocationEstimatedTrucks.selectExpr(
	"sys_row_id as sys_row_id",
	"double ( CaseCube ) as o_CaseCube",
	"double ( WaveCube ) as o_WaveCube",
	"double ( CubicVolume ) as o_CubicVolume"
)

# COMMAND ----------

# Processing node Shortcut_to_IHOP_DETAIL_RPT_PRE, type TARGET 
# COLUMN COUNT: 67

# Joining dataframes SQ_Shortcut_to_AllocationEstimatedTrucks, EXPTRANS to form Shortcut_to_IHOP_DETAIL_RPT_PRE
Shortcut_to_IHOP_DETAIL_RPT_PRE_joined = SQ_Shortcut_to_AllocationEstimatedTrucks.join(EXPTRANS, SQ_Shortcut_to_AllocationEstimatedTrucks.sys_row_id == EXPTRANS.sys_row_id, 'inner')

Shortcut_to_IHOP_DETAIL_RPT_PRE = Shortcut_to_IHOP_DETAIL_RPT_PRE_joined.selectExpr(
	"CAST(Allocationid AS INT) as ALLOCATION_ID",
	"CAST(AllocationQuantity AS INT) as ALLOCATION_QTY",
	"CAST(CapacityWaveEligible AS INT) as CAPACITY_WAVE_ELIGIBLE",
	"CAST(DcId AS INT) as DC_NBR",
	"CAST(EventId AS INT) as EVENT_NBR",
	"CAST(SKU AS INT) as SKU_NBR",
	"CAST(StoreId AS INT) as STORE_NBR",
	"CAST(VendorId AS STRING) as VENDOR_NBR",
	"CAST(WaveTypeId AS INT) as WAVE_TYPE_ID",
	"CAST(AllocationWaveAssignmentId AS INT) as ALLOC_W_ASSIGNMENT_ID",
	"CAST(PrimaryWaveId AS INT) as PRIMARY_WAVE_ID",
	"CAST(NeedByDate AS TIMESTAMP) as ALLOC_W_ASSIGN_NEED_BY_DT",
	"CAST(ArticleName AS STRING) as ARTICLE_NAME",
	"CubicVolume as ARTICLE_CUBIC_VOLUME",
	"CAST(CaseQuantity AS INT) as ARTICLE_CASE_QTY",
	"CAST(SlottingGroupId AS INT) as SLOTTING_GROUP_ID",
	"CAST(CubicVolumeByPieceAndNotCase AS INT) as ARTICLE_CUBIC_VOL_BY_PIECE",
	"CAST(ConstraintTypeId AS INT) as CONSTRAINT_TYPE_ID",
	"CAST(CTypeCode AS INT) as CTYPE_CODE",
	"CAST(CTypeName AS STRING) as CTYPE_NAME",
	"CAST(CTypeDesc AS STRING) as CTYPE_DESC",
	"CAST(IsStoreDefault AS INT) as IS_STORE_DEFAULT",
	"CAST(IsWaveEligible AS INT) as IS_WAVE_ELIGIBLE",
	"CAST(DcName AS STRING) as DC_NAME",
	"CAST(FlaggedForReview AS INT) as DC_FLAGGED_FOR_REVIEW",
	"CAST(DcIsDeleted AS INT) as DC_IS_DELETED",
	"CAST(DLeadTimeInDays AS INT) as DC_TO_STORE_LEAD_TIME_IN_DAYS",
	"CAST(Expected_Delivery_Dt AS INT) as EXPECTEDDELIVERYDT",
	"CAST(PickDayOfWeek AS INT) as PICK_DAY_WK",
	"CAST(ReplenishmentDayOfWeek AS INT) as REPLENISHMENT_DAY_WK",
	"CAST(EventName AS STRING) as EVENT_NAME",
	"CAST(StartDate AS TIMESTAMP) as EVENT_START_DATE",
	"CAST(EndDate AS TIMESTAMP) as EVENT_END_DATE",
	"CAST(DescEvent AS STRING) as EVENT_DESC",
	"CAST(EventParameterId AS INT) as EVENT_PARAMETER_ID",
	"CAST(OB_Alloc_Assig_Delivery_Dt AS TIMESTAMP) as OB_ALLOC_ASSIG_DELIV_DT",
	"CAST(OB_Alloc_Assig_Qty AS INT) as OB_ALLOC_ASSIG_QTY",
	"CAST(StoreDesc AS STRING) as STORE_DESC",
	"CAST(SlottingGroupName AS STRING) as SLOTTING_GROUP_NAME",
	"CAST(StoreHoldOutId AS INT) as STORE_TRANS_ORDER_ID",
	"CAST(SplitOrder AS INT) as STORE_TRANS_ORD_SPLIT",
	"CAST(PurchasingGroup AS INT) as STORE_TRANS_ORD_PG",
	"CAST(ReplenDate AS TIMESTAMP) as STORE_TRANS_ORD_DELIV_DT",
	"CAST(Store_Trans_Orders AS INT) as STORE_TRANS_ORD_QTY",
	"CAST(VenToDcSmoothTypeId AS INT) as VEND_TO_DC_SMOOTH_TYPE_ID",
	"CAST(Vend_Holiday_Lead_Time AS INT) as INCREM_LEAD_TIME_ABOVE_STD",
	"CAST(VendorCapacity AS INT) as VENDOR_CAPACITY",
	"CAST(TruckReleaseDate AS TIMESTAMP) as TRUCK_RELEASE_DATE",
	"CAST(DcHoldOutDate AS TIMESTAMP) as DC_HOLD_OUT_DATE",
	"CAST(VendorName AS STRING) as VENDOR_NAME",
	"CAST(VendorIsDeleted AS INT) as VENDOR_IS_DELETED",
	"CAST(StdLeadTime AS INT) as VENDOR_STD_LEAD_TIME",
	"CAST(VLeadTimeInDays AS INT) as VENDORS_TO_DC_LEAD_TIME_IN_DAYS",
	"CAST(VendToDcSmootingTypeCode AS INT) as VEND_TO_DC_SMOOTH_TYPE_CODE",
	"CAST(VendToDcSmootingTypeName AS STRING) as VEND_TO_DC_SMOOTH_TYPE_NAME",
	"CAST(VendToDcSmootingTypeDesc AS STRING) as VEND_TO_DC_SMOOTH_TYPE_DESC",
	"CAST(WaveId AS INT) as WAVE_ID",
	"CAST(WaveCode AS INT) as WAVE_CODE",
	"CAST(ReleaseDate AS TIMESTAMP) as WAVE_RELEASE_DATE",
	"CAST(IsPrimary AS INT) as WAVE_IS_PRIMARY",
	"CAST(IsVendorManaged AS INT) as WAVE_IS_VENDOR_MANAGED",
	"CAST(PetSmartManaged AS STRING) as PETSMART_MANAGED",
	"CAST(VendorManaged AS STRING) as VENDOR_MANAGED",
	"CAST(WaveTypeCode AS INT) as WAVE_TYPE_CODE",
	"CAST(WaveTypeName AS STRING) as WAVE_TYPE_NAME",
	"CaseCube as CASE_CUBE",
	"WaveCube as WAVE_CUBE"
)
# overwriteDeltaPartition(Shortcut_to_IHOP_DETAIL_RPT_PRE,'DC_NBR',dcnbr,f'{raw}.IHOP_DETAIL_RPT_PRE')
Shortcut_to_IHOP_DETAIL_RPT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.IHOP_DETAIL_RPT_PRE')

# COMMAND ----------


