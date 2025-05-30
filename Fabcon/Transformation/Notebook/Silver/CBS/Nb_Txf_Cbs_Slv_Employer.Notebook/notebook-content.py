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
bkempl_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkempl")
stg_emp_cli_df = spark.read.format("delta").table("BronzeLakehouse.STG_EMP_CLI")
#Gold Tables
bknom_df = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Employer Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating Resultant Dataframe

# CELL ********************

# Alias the DataFrames
bkempl_df = bkempl_df.alias("a")
bknom_df = bknom_df.alias("b1")
stg_emp_cli_df = stg_emp_cli_df.alias("se")

# Apply the transformation
result_df_employer = (
    bkempl_df
    .join(
        bknom_df.filter(col("b1.CTAB") == "040"),
        trim(col("a.CPAY")) == col("b1.CACC"),
        "left"
    )
    .join(
        stg_emp_cli_df,
        col("se.EMP_ID") == trim(col("a.EMP")),
        "left"
    )
    .select(
        trim(col("a.EMP")).alias("EmployerCode"),
        trim(col("a.NOM")).alias("EmployerName"),
        trim(col("a.TYP")).alias("TypeCode"),
        when(trim(col("a.TYP")) == "C", "CLIENT")
            .when(trim(col("a.TYP")) == "T", "Tiers")
            .when(trim(col("a.TYP")) == "A", "ATURE")
            .otherwise("unknown type")
            .alias("TypeDesc"),
        trim(col("a.SIG")).alias("Abbreviation"),
        trim(col("a.CLTI")).alias("LinkedClient"),
        trim(col("a.CPAY")).alias("CountryCode"),
        col("b1.LIB1").alias("Country"),
        trim(col("a.VILLE")).alias("Town"),
        trim(col("a.ADR1")).alias("Address1"),
        trim(col("a.ADR2")).alias("Address2"),
        trim(col("a.ADR3")).alias("Address3"),
        col("se.EMPLOYER_CLI").alias("MANUALEmpCLI"),
        col("a.BATCHID").alias("Batch_ID"),
        col("a.BATCHDATE").alias("Batch_Date"),
        to_date(col("a.BATCHDATE")).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        col("a.SYSTEMCODE").alias("System_Code"),
        lit(None).cast("binary").alias("Row_Hash"),  # Placeholder
        lit("P_Dly_Cbs_Slv_Gld").alias("Workflow_Name")
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

result_df_employer = result_df_employer.withColumn(
    "Row_Hash",
    sha2(
        concat_ws(
            "",
            "EmployerCode",
            "EmployerName",
            "TypeCode",
            "TypeDesc",
            "Abbreviation",
            "LinkedClient",
            "CountryCode",
            "Country",
            "Town",
            "Address1",
            "Address2",
            "Address3",
            "MANUALEmpCLI"
        ),
        256
    ).cast("binary")
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

# ### 4. Writing Data into "**SilverLakehouse.Employer**"

# CELL ********************

final_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsEmployer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
