# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "38062d59-35d8-48a7-b59d-c19d4cd58b52",
# META       "default_lakehouse_name": "RBK_Lakehouse",
# META       "default_lakehouse_workspace_id": "f1bde452-2399-41bc-9ba5-b3c078a190fc",
# META       "known_lakehouses": [
# META         {
# META           "id": "38062d59-35d8-48a7-b59d-c19d4cd58b52"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
df = pd.read_excel(f"{notebookutils.nbResPath}/builtin/My Outlook Send Items.xlsx")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install pyspark pandas openpyxl


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# Load Excel file into a pandas DataFrame
# excel_file_path = "f"{notebookutils.nbResPath}/builtin/My Outlook Send Items.xlsx""
pandas_df = df

# Convert pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower, regexp_replace

# Clean and preprocess text data
spark_df = spark_df.withColumn("Subject", lower(col("Subject")))
spark_df = spark_df.withColumn("Subject", regexp_replace(col("Subject"), "[^a-zA-Z\\s]", ""))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install nltk

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import nltk

# Download the VADER lexicon
nltk.download('vader_lexicon')


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

# CELL ********************

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
import nltk

# Download the VADER lexicon
nltk.download('vader_lexicon')

# Initialize VADER sentiment analyzer
sia = SentimentIntensityAnalyzer()

# Define a UDF to get the compound sentiment score
def get_sentiment(text):
    return float(sia.polarity_scores(text)["compound"])

sentiment_udf = udf(get_sentiment, FloatType())

# Apply the UDF to the DataFrame
spark_df = spark_df.withColumn("sentiment", sentiment_udf(col("Subject")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert Spark DataFrame back to pandas DataFrame
result_df = spark_df.toPandas()

# Save to Excel
result_df.to_excel(f"{notebookutils.nbResPath}/builtin/result.xlsx", index=False)

# Display the results
result_df.head()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
