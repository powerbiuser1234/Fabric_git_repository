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

# ### **1. Libraries**

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **2. Reading Tables**

# CELL ********************

bkprod = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkprod")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **3. ProductDim Dataframe**

# CELL ********************

ProductDim_df = (
    bkprod
    .select(
        trim(bkprod.CPRO).alias("ProductCode"),
        trim(bkprod.LIB).alias("ProductDescription"),
        bkprod.DOU.alias("ProdCreatedOn"),
        bkprod.DMO.alias("ProdUpdatedOn"),
        bkprod.BATCHID.alias("Batch_ID"),
        bkprod.BATCHDATE.alias("Batch_Date"),
        to_date(bkprod.BATCHDATE).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        bkprod.SYSTEMCODE.alias("System_Code"),
        lit(None).cast("binary").alias("Row_Hash"),  
        bkprod.WORKFLOWNAME.alias("Workflow_Name")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ProductDim_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3.1 Adding values into Rowhash column**

# CELL ********************


ProductDim_df = ProductDim_df.withColumn(
    "Row_Hash",
    sha2(concat_ws("",
        col("ProductCode"),
        col("ProductDescription"),
        col("ProdCreatedOn").cast("string"),
        col("ProdUpdatedOn").cast("string")
    ), 256)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **4. Writing data into SilverLakehouse.Cbs_Productdim**

# CELL ********************

ProductDim_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.Cbs_ProductDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
