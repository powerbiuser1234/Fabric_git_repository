# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "197819da-2cec-4c3c-a96a-0fe62ae2300b",
# META       "known_lakehouses": [
# META         {
# META           "id": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.sql('select count(*) zero_count from BronzeLakehouse.cl_bronze_count_comparison where row_counts = 0')
zero_check = df_check.collect()[0]['zero_count']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check2 = spark.sql('select count(*) full_row_duplication from BronzeLakehouse.cl_bronze_count_comparison where counts_difference <> 0')
fldup_check = df_check2.collect()[0]['full_row_duplication']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

if zero_check == 0 and fldup_check == 0:
    result = "pass"
else:
    result = "fail"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
