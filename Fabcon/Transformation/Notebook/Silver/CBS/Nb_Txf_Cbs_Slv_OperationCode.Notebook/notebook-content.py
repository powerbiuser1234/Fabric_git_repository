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

# MARKDOWN ********************

# #### **1. Libraries**

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **2. Reading Tables**

# CELL ********************

bkope = spark.read.format("delta").table("BronzeLakehosue.Cbs_Bkope")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3. OperationCode ETL**

# CELL ********************

OperationCode_df = (
    bkope
    .select(
        trim(bkope.AGE).alias("AgenceCode"),
        trim(bkope.OPE).alias("OperationCode"),
        trim(bkope.LIB).alias("OperationDesc"),
        bkope.DOU.alias("CREATEDATE"),
        bkope.DMO.alias("UPDATEDATE"),
        trim(bkope.UTI).alias("USERCODE"),
        bkope.BATCHID.alias("Batch_ID"),
        bkope.BATCHDATE.alias("Batch_Date"),
        to_date(bkope.BATCHDATE).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        bkope.SYSTEMCODE.alias("System_Code"),
        bkope.WORKFLOWNAME.alias("Workflow_Name")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **4. Writing data into SilverLakehouse.Cbs_OperationCode**

# CELL ********************


# bkope_df.write.format("delta").mode("append").saveAsTable("SilverLakehouse.Cbs_OperationCode")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
