# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ef516724-eca9-40d5-92bc-40212bb6944e",
# META       "default_lakehouse_name": "GoldLakehouse",
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
# META       "known_lakehouses": [
# META         {
# META           "id": "e56ddaf5-2e15-4634-91cc-e2eb818afa61"
# META         },
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ClearingDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim")
ClearingSK = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/dbo_clearingsk")
CL_BKCOMPENS_RF = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RF")
Branch = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/dbo_branch")
ClearingStatus = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/dbo_clearingstatus")
CL_BKCOMPENS_RF_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RF_ETA")
CL_BKCOMPENS_RV_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_ETA")
CL_BKCOMPENS_RV_CHQ = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_CHQ")
CL_BKCOMPENS_RV_TRF = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_TRF")
CL_BKCOMPENS_AV = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV")
CL_BKCOMPENS_AF = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF")
CL_BKCOMPENS_AF_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF_ETA")
CL_BKCOMPENS_AV_CHQ = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_CHQ")
CL_BKCOMPENS_AF_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF_ETA")
CL_BKCOMPENS_RV = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV")
CL_BKCOMPENS_AV_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_ETA")
CL_BKCOMPENS_AV_TRF = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_TRF")
ClearingDim_H = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim_H")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ClearingSK)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp_RV = (CL_BKCOMPENS_RV
           .select(F.lit('R').alias("Hardcoded"), F.col("IDRV"))
           .distinct())

temp_AV = (CL_BKCOMPENS_AV
           .select(F.lit('A').alias("Hardcoded"), F.col("IDAV"))
           .distinct())  #alias to IDRV so union can be performed

temp = temp_RV.union(temp_AV)
#temp = temp.withColumn("IDRV", F.col("IDRV").cast(StringType()))
#ClearingSK = ClearingSK.withColumn("ClearingCode", F.col("ClearingCode").cast(StringType()))

joined_df = (temp.alias("temp")
             .join(ClearingSK.alias("DW"),
                   (F.col("temp.Hardcoded") == F.col("DW.ClearingType")) & 
                   (F.col("temp.IDRV") == F.col("DW.ClearingCode")),
                   "left")
             .filter(F.col("DW.ClearingType").isNull() & F.col("DW.ClearingCode").isNull())
             .select(F.col("temp.Hardcoded").alias("ClearingType"), F.col("temp.IDRV").alias("ClearingCode")))

display(joined_df)
# Writing the final result back to the ClearingSK table
#joined_df.write.format("delta").mode("append").save("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingSKabfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/dbo_clearingsk")

#joined_df.write.mode("append").saveAsTable("GoldLakehouse.dbo_clearingsk")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
