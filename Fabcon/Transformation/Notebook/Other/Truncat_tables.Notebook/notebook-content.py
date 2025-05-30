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

# CELL ********************

tables_to_truncate = [
    "TEMP_BKCOMPENS_AV",
    "TEMP_BKCOMPENS_AV_EVEC",
    "TEMP_BKCOMPENS_AV_EVE",
    "TEMP_BKCOMPENS_RV_EVE",
    "TEMP_BKCOMPENS_RV_CHQ",
    "TEMP_BKCOMPENS_RV",
    "TEMP_BKCOMPENS_RV_ETA",
    "TEMP_BKCOMPENS_RF_ETA",
    "TEMP_BKCOMPENS_RF",
    "TEMP_BKCOMPENS_ERROR",
    "TEMP_BKCOMPENS_AV_CHQ",
    "TEMP_BKCOMPENS_AF",
    "TEMP_BKCOMPENS_AV_ETA",
    "TEMP_BKCOMPENS_AF_ETA",
    "TEMP_BKCOMPENS_RV_TRF",
    "TEMP_BKCOMPENS_AV_TRF"
]

schema_name = "BronzeLakehouse"

for table in tables_to_truncate:
    full_table_name = f"{schema_name}.{table}"
    print(f"Deleting all rows from table: {full_table_name}")
    spark.sql(f"DELETE FROM {full_table_name} WHERE TRUE")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
