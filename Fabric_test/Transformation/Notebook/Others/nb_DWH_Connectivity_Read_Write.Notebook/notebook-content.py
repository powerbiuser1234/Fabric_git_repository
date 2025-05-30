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
# META           "id": "b554114f-f6f1-42fa-b182-35c9b03d7bfd"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "ea99d35f-f18c-b0bb-4ae5-91f1ef274073",
# META       "known_warehouses": [
# META         {
# META           "id": "ea99d35f-f18c-b0bb-4ae5-91f1ef274073",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# Read data from Lakehouse table
df = spark.read.format("delta").load("Tables/Cl_Bkcompens_Av")

# Write DataFrame to Fabric Data Warehouse
df.write.mode("overwrite").synapsesql("DataOpsWarehouse.dbo.Cl_Bkcompens_Av")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
# Read from Fabric Warehouse table
df_read = spark.read.synapsesql("DataOpsWarehouse.dbo.Cl_Bkcompens_Av")

# Show sample records
df_read.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
