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
from pyspark.sql.types import *
from pyspark.sql.window import *
from delta.tables import *

#spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
#spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **2. Reading Tables**

# CELL ********************

evuti = spark.read.format("delta").table("BronzeLakehouse.Cbs_Evuti")
bknom=spark.read.format("delta").table("BronzeLakehouse.Cbs_Bknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **3. Users ETL**

# MARKDOWN ********************

# ##### **3.1 Users Dataframe**

# CELL ********************

Users_df = (
    evuti
    .join(bknom.alias("B1"), (evuti.AGE == col("B1.CACC")) & (col("B1.CTAB") == '001'), "left")
    .join(bknom.alias("B2"), (evuti.SER == col("B2.CACC")) & (col("B2.CTAB") == '034'), "left")
    .select(
        trim(evuti.AGE).alias("UserBranchCode"),
        trim(col("B1.LIB1")).alias("UserBranch"),
        trim(evuti.CUTI).alias("UserCode"),
        trim(evuti.LIB).alias("UserName"),
        trim(evuti.CLI).alias("ClientCode"),
        trim(evuti.DECI).alias("ClientID"),
        evuti.DOU.alias("IsBrManager"),
        evuti.DMO.alias("CreatedDate"),
        trim(evuti.SER).alias("DeptCode"),
        trim(col("B2.LIB1")).alias("DeptName"),
        when(trim(evuti.SUS) == 'O', lit('Oui')).otherwise(lit('Non')).alias("SuspensionCode"),
        evuti.BatchID,
        evuti.BatchDATE,
        evuti.BatchDATE.alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        evuti.SYSTEMCODE,
        lit(None).cast("binary").alias("RowHash"),
        evuti.WORKFLOWNAME
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **3.2 Adding values to RowHash**

# CELL ********************


Users_df = Users_df.withColumn(
    "RowHash",
    sha2(
        concat_ws("",
            col("UserBranchCode"),
            col("UserBranch"),
            col("UserCode"),
            col("UserName"),
            col("ClientCode"),
            col("ClientID"),
            col("IsBrManager"),
            col("CreatedDate"),
            col("UpdatedDate"),
            col("DeptCode"),
            col("DeptName"),
            col("SuspensionCode")), 256).cast("binary"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(Users_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.caseSensitive", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **4. Writing Data into SilverLakehouse.Users**

# CELL ********************

#Users_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.Users")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
