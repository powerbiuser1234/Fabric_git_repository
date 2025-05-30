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

# ### 1. Libraries

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType, BinaryType, DateType, TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Reading tables

# CELL ********************

#Bronze Tables
bkautc_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkautc")
evuti_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Evuti")
overdraft_status_df = spark.read.format("delta").table("BronzeLakehouse.OverdraftStatus")
#Gold Tables
bknom_df = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

max_batch_date = bkautc_df.selectExpr("cast(max(BATCHDATE) as date) as max_date").collect()[0]["max_date"]

# Filter BKAUTC for MAX BATCH DATE
bkautc_df = bkautc_df.filter(to_date(col("BATCHDATE")) == max_batch_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# trimmed columns from BKAUTC
bkautc_df = bkautc_df.withColumn("AGE", trim(col("AGE"))) \
    .withColumn("NAUT", trim(col("NAUT"))) \
    .withColumn("UTA", trim(col("UTA"))) \
    .withColumn("UTF", trim(col("UTF"))) \
    .withColumn("UTI", trim(col("UTI"))) \
    .withColumn("SIT", trim(col("SIT"))) \
    .withColumn("NAT", trim(col("NAT"))) \
    .withColumn("EVE", trim(col("EVE"))) \
    .withColumn("ETA", trim(col("ETA"))) \
    .withColumn("OPE", trim(col("OPE"))) \
    .withColumn("RENOUV", trim(col("RENOUV")))

# trimmed columns from EVUTI 
evuti_df = evuti_df.withColumn("CUTI", trim(col("CUTI"))) \
    .withColumn("LIB", trim(col("LIB")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. OverdraftDim Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating Resultant Dataframe

# CELL ********************

overdraft_dim_df  = (
    bkautc_df
    .join(evuti_df.alias("e1"), col("UTA") == col("e1.cuti"), "left")
    .join(evuti_df.alias("e2"), col("UTF") == col("e2.cuti"), "left")
    .join(evuti_df.alias("e3"), col("UTI") == col("e3.cuti"), "left")
    .join(bknom_df.filter(col("CTAB") == "001").alias("b1"), col("AGE") == col("b1.CACC"), "left")
    .join(overdraft_status_df.alias("os"), col("ETA") == col("os.OverdraftStatusCode"), "left")
    .join(bknom_df.filter(col("CTAB") == "052").alias("b2"), col("NAT") == col("b2.CACC"), "left")
    .select(
    col("NAUT").alias("OverdraftNumber"),
    col("DOU").alias("ODCreateDate"),
    col("DMO").alias("ODUpdateDate"),
    col("DUREE").alias("Duration"),
    col("NORD").alias("OrderNumber"),
    col("TCOMN").alias("CommissionRate"),
    col("RENOUV").alias("IsRecurring"),
    col("UTA").alias("AuthUserCode"),
    col("UTF").alias("OverrideUserCode"),
    col("UTI").alias("InitiateUserCode"),
    col("e1.LIB").alias("AuthUser"),
    col("e2.LIB").alias("OverrideUser"),
    col("e3.LIB").alias("InitiateUser"),
    col("EVE").alias("EventNumber"),
    col("AGE").alias("BranchCode"),
    col("b1.LIB1").alias("Branch"),
    col("os.OverdraftStatusID").alias("ODStatusID"),
    col("OPE").alias("OperationCode"),
    
    # New columns
    col("NAT").alias("TranNatureCode"),
    col("b2.LIB1").alias("TranNatureDesc"),
    col("SIT").alias("ODLimitSITCode"),
    when(col("SIT") == "O", "Ouvete")
        .when(col("SIT") == "M", "Modifiée")
        .when(col("SIT") == "A", "Annulé")
        .otherwise("Inconnue")
        .alias("ODLimitSITDesc"),

    # Common metadata
    col("BATCHID").alias("Batch_ID"),
    col("BATCHDATE").alias("Batch_Date"),
    to_date("BATCHDATE").alias("Created_On"),
    lit(None).cast("timestamp").alias("Updated_On"),
    col("SYSTEMCODE").alias("System_Code"),
    lit(None).cast("binary").alias("Row_Hash"),
    lit("P_Dly_Cbs_Slv_Gld").alias("WorkflowName")
)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3.2 Adding Values to RowHash Column

# CELL ********************

overdraft_dim_df = overdraft_dim_df.withColumn(
    "Row_Hash",
    sha2(
        concat_ws(
            ",",  # Match SQL's CONCAT_WS(',', ...)
            col("OverdraftNumber"),
            col("ODCreateDate"),
            col("ODUpdateDate"),
            col("Duration"),
            col("OrderNumber"),
            col("CommissionRate"),
            col("IsRecurring"),
            col("AuthUserCode"),
            col("OverrideUserCode"),
            col("InitiateUserCode"),
            col("AuthUser"),
            col("OverrideUser"),
            col("InitiateUser"),
            col("EventNumber"),
            col("BranchCode"),
            col("Branch"),
            col("ODStatusID"),
            col("OperationCode"),
            # New columns
            col("TranNatureCode"),
            col("TranNatureDesc"),
            col("ODLimitSITCode"),
            col("ODLimitSITDesc")
        ),
        256
    )
)


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

# ### 4. Writing Data into "**SilverLakehouse.OverdraftDim**"

# CELL ********************

overdraft_dim_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsOverdraftDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
