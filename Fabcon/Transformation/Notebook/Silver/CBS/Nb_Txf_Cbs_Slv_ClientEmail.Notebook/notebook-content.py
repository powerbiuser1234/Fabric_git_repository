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
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         },
# META         {
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### **1. Library**

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **2. Reading Tables**

# CELL ********************

#from Bronzelakehouse
bkemacli = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkemacli")
bknom = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bknom")

#from GoldLakehouse
client_sk = spark.read.format("delta").table("GoldLakehouse.Cbs_ClientSk")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **3. Client Email DataFrame**

# CELL ********************

clientEmail_df = (
    bkemacli
    .filter(trim(col("CLI")) != '')  
    .join(client_sk, trim(bkemacli.CLI) == client_sk.SourceID, "left")
    .join(bknom.alias("B1"), (trim(bkemacli.TYP) == col("B1.CACC")) & (col("B1.CTAB") == '183'), "left")
    .filter(client_sk.SourceID.isNotNull())  
    .select(
        client_sk.ClientID,
        trim(bkemacli.CLI).alias("ClientCode"),
        trim(bkemacli.TYP).alias("FmtTypeCode"),
        trim(col("B1.LIB1")).alias("FormatType"),
        trim(bkemacli.EMAIL).alias("ContactNumber"),
        bkemacli.BATCHID.alias("BatchID"),
        bkemacli.BATCHDATE.alias("BatchDate"),
        to_date(bkemacli.BATCHDATE).alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        bkemacli.SYSTEMCODE.alias("SystemCode"),
        lit(None).cast("binary").alias("RowHash"),
        bkemacli.WORKFLOWNAME.alias("WorkflowName")
    )
)a

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **3.1 Adding values to Rowhash Column**

# CELL ********************

clientEmail_df = clientEmail_df.withColumn(
    "RowHash",
    sha2(concat_ws(",", 
        col("ClientID"),
        col("ClientCode"),
        col("FmtTypeCode"),
        col("FormatType"),
        col("ContactNumber")
    ), 256).cast("binary"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(clientEmail_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **4. Write data into SilverLakehouse.Cbs_ClientEmail**

# CELL ********************

clientEmail_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.Cbs_ClientEmail")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
