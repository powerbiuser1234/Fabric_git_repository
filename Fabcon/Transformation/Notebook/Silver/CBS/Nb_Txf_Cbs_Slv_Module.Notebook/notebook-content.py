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

bknom = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3. Module ETL**

# CELL ********************

DisapprovalType_df=bknom.filter(bknom.CTAB == '127').select( 
    trim(bknom.CACC).alias("ModuleCode"),
    trim(bknom.LIB1).alias("ModuleName"),
    bknom.BatchID.alias("Batch_ID"),
    bknom.BATCHDATE.alias("Batch_Date"),
    to_date(bknom.BATCHDATE).alias("Created_On"),
    lit(None).cast("date").alias("Updated_On"),
    bknom.SystemCode.alias("System_Code"),
    lit(None).cast("binary").alias("Row_Hash"),
    bknom.WorkFlowName.alias("Workflow_Name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **4. Writing Data into SilverLakehouse.CbsModule**

# CELL ********************

DisapprovalType_df.write.format("delta").mode("append").saveAsTable("SilverLakehouse.CbsModule")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
