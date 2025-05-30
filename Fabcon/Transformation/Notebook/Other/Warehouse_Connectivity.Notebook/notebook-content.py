# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a43617b8-dd1a-46c9-aa3c-c2645e30c949",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
# META       "known_lakehouses": [
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

#Read from warehouse
df = spark.read.synapsesql("DataOpsWarehouse.CFR.BatchProcessingLog_Bkp")

#Write to lakehouse
df.write.format("delta").saveAsTable("BronzeLakehouse.sample_table1")

#Write to Warehouse
df.write.mode("overwrite").synapsesql("DataOpsWarehouse.CFR.sample_table2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.synapsesql("DataOpsWarehouse.CFR.BatchProcessingLog_Bkp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df = spark.read.synapsesql("<warehouse/lakehouse name>.<schema name>.<table or view name>")
df.write.format("delta").saveAsTable("BronzeLakehouse.sample_table1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").synapsesql("DataOpsWarehouse.CFR.sample_table2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
