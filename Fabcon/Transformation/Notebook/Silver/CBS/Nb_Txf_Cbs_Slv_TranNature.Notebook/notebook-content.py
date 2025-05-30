# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

bknom = spark.read.format("delta").table("GoldLakehouse.Cbs_Bknom")# we will be using bknom from gold lakehouse
cms_transaction_code=spark.read.format("delta").table("BronzeLakehouse.CMS_TransactionCode")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3. TranNature ETL**

# MARKDOWN ********************

# ###### **3.1 TranNature CBS DF**

# CELL ********************

tran_nature_cbs_df = (
    bknom.filter(trim(bknom.CTAB) == '052')
    .select(
        trim(bknom.CACC).alias("NatureCode"),
        trim(bknom.LIB1).alias("NatureDesc"),
        lit("CBS").alias("NatureSource"),
        bknom.BATCHID.alias("Batch_ID"),
        bknom.BATCHDATE.alias("Batch_Date"),
        to_date(bknom.BATCHDATE).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        bknom.SYSTEMCODE.alias("System_Code"),
        bknom.WORKFLOWNAME.alias("Workflow_Name")
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **3.2 TranNature CMS DF**

# CELL ********************

tran_nature_cms_df = (
    cms_transactioncode.select(
        trim(cms_transactioncode.TCO_CODE).alias("NatureCode"),
        trim(cms_transactioncode.TCO_LABE).alias("NatureDesc"),
        lit("CMS").alias("NatureSource"),
        cms_transactioncode.BATCHID.alias("Batch_ID"),
        cms_transactioncode.BATCHDATE.alias("Batch_Date"),
        to_date(cms_transactioncode.BATCHDATE).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        cms_transactioncode.SYSTEMCODE.alias("System_Code"),
        cms_transactioncode.WORKFLOWNAME.alias("Workflow_Name")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **3.3 Union on both DF**

# CELL ********************

tran_nature_df = tran_nature_cbs_df.union(tran_nature_cms_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  #### **4. Writing Data into Silver.Cbs_TranNature**

# CELL ********************

tran_nature_df.write.format("delta").mode("append").saveAsTable("SilverLakehouse.CbsTranNature")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
