# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b554114f-f6f1-42fa-b182-35c9b03d7bfd",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "4f2aaff8-44cb-476d-805b-1c95e486af08",
# META       "known_lakehouses": [
# META         {
# META           "id": "5953e930-bc05-403f-8cad-ba3d204e63d3"
# META         },
# META         {
# META           "id": "7ba102aa-777a-4ad5-8f68-ce62922c90b3"
# META         },
# META         {
# META           "id": "b554114f-f6f1-42fa-b182-35c9b03d7bfd"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 1. Libraries

# CELL ********************

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Creating similar logic like Identity Column

# CELL ********************

#Date Varibales
# MaxBatchDate = spark.sql('Select DISTINCT Cast(Batch_Date as date) from SilverLakehouse.ClearingDim_H').collect()[0]['Batch_Date']
# print(MaxBatchDate)

# MaxClearingIDH = spark.sql('Select Max(ClearingID_H) as ClearingID_H from GoldLakehouse.ClearingDim_H').collect()[0]['ClearingID_H']
# print(MaxClearingIDH)

# Add surrogate key to new records
# windowSpec = Window.orderBy("ClearingID")
# clearing_dim = clearing_dim.withColumn("ClearingID_H", (row_number().over(windowSpec) + max_clearingid_h).cast(LongType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Reading Tables

# CELL ********************

delta_dw = DeltaTable.forName(spark, "GoldLakehouse.ClearingDim")
df_dw = delta_dw.toDF()

delta_dw_h = DeltaTable.forName(spark, "GoldLakehouse.ClearingDim_H")
df_dw_h = delta_dw_h.toDF()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# windowSpec = Window.orderBy('ClearingID')  
# .withColumn("ClearingID_H", (row_number().over(windowSpec) + MaxClearingIDH).cast(LongType()))
# transformed_df = (
#     df_dw
#     .filter(to_date(col("Batch_Date")) == MaxBatchDate)  # Apply filter for MaxBatchDate
#     .withColumnRenamed("Row_Hash", "SCD_RowHash")      # Rename Row_Hash to SCD_RowHash
#     .withColumn("ClearingID_H", (row_number().over(windowSpec) + MaxClearingIDH).cast(LongType()))
#     .withColumn("SCD_StartDate", col("Batch_Date"))    # Add SCD_StartDate as Batch_Date
#     .withColumn("SCD_EndDate", to_timestamp(lit("9999-12-31")))      # Add SCD_EndDate as a fixed date
#     .select('AccountID_H', 'AccountID','AccountCode','BranchCode','Branch','AccNumberMXP','BankAccNumber','AccountCategory','BankAccTypeCode','BankAccType','CurrencyCode','CurrencyISO','LatePayStatus','LatePayDate','UnpaidStatus','UnpaidDate','OverlineStatus','OverlineDate','CreatedDate','EffectiveDate','TotalAmtAuth','AmountUsed','AccCloseDate','AccountTypeCode','AccountType','AccountStatusCode','AccountStatus','CustomerCode','MerchantCode','Batch_ID','Batch_Date','Created_On','Updated_On','System_Code','SCD_RowHash','SCD_StartDate','SCD_EndDate','Workflow_Name') 
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge statement to handle SCD Type 2
(delta_dw_h.alias("H")
 .merge(
     df_dw.alias("DW"), "H.ClearingID = DW.ClearingID AND H.ScdEndDate = '9999-12-31'"
 )
 .whenMatchedUpdate(
     condition="H.RowHash <> DW.RowHash",
     set={
         "ScdEndDate": date_sub(col("DW.BatchDate"), 1)
     }
 )
 .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge statement to handle SCD Type 2
(delta_dw_h.alias("H")
 .merge(
     df_dw.alias("DW"), "H.ClearingID = DW.ClearingID AND H.ScdEndDate = '9999-12-31'"
 )
 
 .whenNotMatchedInsert(
     values={
         "ClearingID": col("DW.ClearingID"),
         "ClearingType": col("DW.ClearingType"),
         "ClearingCode": col("DW.ClearingCode"),
         "FileBranchCode": col("DW.FileBranchCode"),
         "FileBranch": col("DW.FileBranch"),
         "FileCode": col("DW.FileCode"),
         "FileStatus": col("DW.FileStatus"),
         "FileStatusDesc": col("DW.FileStatusDesc"),
         "FileReference": col("DW.FileReference"),
         "FileName": col("DW.FileName"),
         "FileStatusDate": col("DW.FileStatusDate"),
         "ClearingStatus": col("DW.ClearingStatus"),
         "ClearingStatusDesc": col("DW.ClearingStatusDesc"),
         "InstrumentRef": col("DW.InstrumentRef"),
         "SenderInfo": col("DW.SenderInfo"),
         "ReceiverInformation": col("DW.ReceiverInformation"),
         "CheckDigit": col("DW.CheckDigit"),
         "ReceiverName": col("DW.ReceiverName"),
         "TranRef": col("DW.TranRef"),
         "ClearingDate": col("DW.ClearingDate"),
         "ClearingStatusReason": col("DW.ClearingStatusReason"),
         "ChequeID": col("DW.ChequeID"),
         "ChqVisStatus": col("DW.ChqVisStatus"),
         "ChqNumber": col("DW.ChqNumber"),
         "Narration": col("DW.Narration"),
         "BatchID": col("DW.BatchID"),
         "BatchDate": col("DW.BatchDate"),
         "CreatedOn": lit(None),
         "UpdatedOn": lit(None),
         "SystemCode": col("DW.SystemCode"),
         "RowHash": col("DW.RowHash"),
         "ScdStartDate": col("DW.BatchDate"),
         "ScdEndDate": lit("9999-12-31"),
         "WorkFlowName": col("DW.WorkFlowName")
     }
 )
 .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from GoldLakehouse.ClearingDim_H

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select cast(BatchDate as date), UpdatedOn, ScdEndDate, count(*) from GoldLakehouse.ClearingDim_H 
# MAGIC group by cast(BatchDate as date) , UpdatedOn, ScdEndDate
# MAGIC order by cast(BatchDate as date), ScdEndDate ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select cast(CreatedOn as date), UpdatedOn, count(*) from GoldLakehouse.ClearingDim
# MAGIC group by cast(CreatedOn as date) , UpdatedOn
# MAGIC order by cast(CreatedOn as date) ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
