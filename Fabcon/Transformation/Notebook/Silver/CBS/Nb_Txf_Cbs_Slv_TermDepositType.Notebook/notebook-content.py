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

bknom = spark.read.format("delta").table("GoldLakehouse.Cbs_Bknom")# use bknom from gold
bkprod = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkprod")
bktydat = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bktydat")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3. Term Deposit Type ETL**

# CELL ********************


term_deposit_type_df = (
    bktydat
    .join(bknom,(trim(bktydat.AGE) == trim(bknom.CACC)) & (trim(bknom.CTAB) == '001'),"left")
    .join(bkprod,trim(bktydat.CPRO) == trim(bkprod.CPRO),"left")
    .select(
        trim(bktydat.TYP).alias("TypeCode"),
        trim(bktydat.LIBE).alias("TypeDescription"),
        trim(bktydat.AGE).alias("BranchCode"),
        trim(bknom.LIB1).alias("Branch"),
        trim(bktydat.CPRO).alias("ProductCode"),
        trim(bkprod.LIB).alias("ProductName"),
        bktydat.DOU.alias("TypeCreatedOn"),
        bktydat.DMO.alias("TypeUpdatedOn"),
        bktydat.BATCHID.alias("Batch_ID"),
        bktydat.BATCHDATE.alias("Batch_Date"),
        to_date(bktydat.BATCHDATE).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        bktydat.SYSTEMCODE.alias("System_Code"),
        bktydat.WORKFLOWNAME.alias("Workflow_Name")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **4. Writing Data into SilverLakehouse.CbsTermDepositType****

# CELL ********************

term_deposit_type_df.write.format("delta").mode("append").saveAsTable("SilverLakehouse.CbsTermDepositType")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
