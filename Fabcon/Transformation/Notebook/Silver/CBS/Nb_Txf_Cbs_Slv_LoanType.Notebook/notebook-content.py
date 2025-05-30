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

# #### **2. Reading Table**

# CELL ********************

bktyprt = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bktyprt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3. Loan Type ETL**

# CELL ********************

loanType_df = (
    bktyprt
    .select(
        trim(bktyprt.AGE).alias("AgenceCode"),
        trim(bktyprt.TYP).alias("TypeCode"),
        trim(bktyprt.LIBE).alias("TypeDesc"),
        bktyprt.BATCHID.alias("Batch_ID"),
        bktyprt.BATCHDATE.alias("Batch_Date"),
        to_date(bktyprt.BATCHDATE).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        bktyprt.SYSTEMCODE.alias("System_Code"),
        bktyprt.WORKFLOWNAME.alias("Workflow_Name")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **4. Writing Data into SilverLakehouse.Cbs_Loantype**

# CELL ********************

loanType_df.write.format("delta").mode("Overwrite").saveAsTable("BronzeLakehouse.Cbs_LoanType")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
