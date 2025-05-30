# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03",
# META       "default_lakehouse_name": "SilverLakehouse",
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
# META       "known_lakehouses": [
# META         {
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         },
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         }
# META       ]
# META     }
# META   }
# META }

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

# CELL ********************

#Bronze Tables
Cbs_Bktyaut = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bktyaut")
# Perform the equivalent transformations in PySpark
Cbs_Bktyaut = Cbs_Bktyaut.select(
    'DOU',
    'DMO',
    trim(col("AGE")).alias("AGE"),
    trim(col("TYP")).alias("TYP"),
    trim(col("LIBE")).alias("LIBE"),
    trim(col("OPE")).alias("OPE"),
    trim(col("UTI")).alias("UTI"),
    trim(col("ATRF")).alias("ATRF"),
    trim(col("CPRO")).alias("CPRO"),
    trim(col("TEG_ARR")).alias("TEG_ARR"),
    trim(col("CALCDEC")).alias("CALCDEC"),
    'BatchID',
    'BatchDATE',
    'SystemCode'
)

Cbs_Evuti = spark.read.format("delta").table("BronzeLakehouse.Cbs_Evuti")
# Perform the equivalent transformations in PySpark
Cbs_Evuti = Cbs_Evuti.select(
    trim(col("CUTI")).alias("CUTI"),
    trim(col("LIB")).alias("LIB")
)


#Gold Tables
CbsBknom = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

CbsBranch = spark.read.format("delta").table("GoldLakehouse.CbsBranch")

CbsUsers = spark.read.format("delta").table("GoldLakehouse.CbsUsers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
