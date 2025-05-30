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
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 1. Libraries

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from delta.tables import *

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

bkcom = spark.table("BronzeLakehouse.Cbs_Bkcom")
cms_account = spark.table("BronzeLakehouse.Cbs_CMS_Account")
account_sk = spark.table("GoldLakehouse.Cbs_AccountSK")
currency = spark.table("GoldLakehouse.Cbs_Currency")
bknom = spark.table("GoldLakehouse.Cbs_BKNOM")
bkchap = spark.table("BronzeLakehouse.Cbs_Bkchap")
client_sk = spark.table("GoldLakehouse.Cbs_ClientSK")
evuti = spark.table("BronzeLakehouse.Cbs_EVUTI")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. AccountSK ETL

# CELL ********************

currency_df = currency.select(trim("CurrencyCode").alias("CurrencyCodeCMS"))

account_sk_insert = (
    bkcom.alias("ACC")
    .join(account_sk.alias("ASK"),
          (col("ACC.AGE") == col("ASK.BranchCode")) &
          (col("ACC.DEV") == col("ASK.CurrencyCode")) &
          (col("ACC.NCP") == col("ASK.AccountNumber")),
          "left")
    .join(currency_df.alias("cur"),
          col("ACC.DEV") == col("cur.CurrencyCodeCMS"),
          "left")
    .join(cms_filtered.alias("CMSACC"),
          (col("CMSACC.ACC_NUMB") == concat_ws("", col("ACC.AGE"), col("ACC.NCP"))) &
          (col("CMSACC.ACC_CURR_CODE") == col("cur.CurrencyCodeCMS")),
          "left")
    .filter(col("ASK.BranchCode").isNull() &
            col("ASK.CurrencyCode").isNull() &
            col("ASK.AccountNumber").isNull())
    .select(
        col("ACC.AGE").alias("BranchCode"),
        col("ACC.DEV").alias("CurrencyCode"),
        col("ACC.NCP").alias("AccountNumber"),
        col("CMSACC.ACC_CODE").cast("string").alias("CMSAccNumber")
    )
)

# Write to AccountSK
# account_sk_insert.write.insertInto("GoldLakehouse.Cbs_AccountSK")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. AccountDim ETL

# MARKDOWN ********************

# #### 3.1 Creating Resultant Dataframe

# CELL ********************

max_batch_date = bkcom.agg({"BATCHDATE": "max"}).collect()[0][0]

b1 = bknom.filter(col("CTAB") == '001').select("CACC", "LIB1").withColumnRenamed("LIB1", "LIB1_B1")
b2 = bknom.filter(col("CTAB") == '001').select("CACC", "LIB1").withColumnRenamed("LIB1", "LIB1_B2")
b3 = bknom.filter(col("CTAB") == '001').select("CACC", "LIB1").withColumnRenamed("LIB1", "LIB1_B3")
b4 = bknom.filter(col("CTAB") == '005').select("CACC", "LIB1").withColumnRenamed("LIB1", "LIB1_B4")
b5 = bknom.filter(col("CTAB") == '005').select("CACC", "LIB2")
b6 = bknom.filter(col("CTAB") == '032').select("CACC", "LIB1").withColumnRenamed("LIB1", "LIB1_B6")

accdim_df = (
    bkcom.alias("ACC")
    .filter(col("ACC.BATCHDATE") == lit(max_batch_date))
    .join(account_sk.alias("ACCSK"),
          (col("ACC.NCP") == col("ACCSK.AccountNumber")) &
          (col("ACC.DEV") == col("ACCSK.CurrencyCode")) &
          (col("ACC.AGE") == col("ACCSK.BranchCode")),
          "left")
    .join(b1, col("ACC.AGE") == col("B1.CACC"), "left")
    .join(b2, col("ACC.AGECRE") == col("B2.CACC"), "left")
    .join(bkchap.alias("C"), col("ACC.CHA") == col("C.CHA"), "left")
    .join(client_sk.alias("CLISK"), col("ACC.CLI") == col("CLISK.SourceID"), "left")
    .join(b3, col("ACC.DERNAGE") == col("B3.CACC"), "left")
    .join(b4, col("ACC.DERNDEV") == col("B4.CACC"), "left")
    .join(b5, col("ACC.DEV") == col("B5.CACC"), "left")
    .join(b6, col("ACC.TYP") == col("B6.CACC"), "left")
    .join(evuti.alias("E1"), col("ACC.UTI") == col("E1.CUTI"), "left")
    .join(evuti.alias("E2"), col("ACC.UTIC") == col("E2.CUTI"), "left")
    .withColumn("CREATEDON", to_date(col("ACC.BATCHDATE")))
    .withColumn("Updated_On", lit(None).cast("date"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3.2 Adding Values to RowHash Column

# CELL ********************

from pyspark.sql.functions import sha2, concat_ws, col

# List of columns involved in the hash computation
hash_columns = [
    "AccountID",
    "AccAgenceCode",
    "AccAgence",
    "AccCreateAgenceCode",
    "AccCreateAgence",
    "InterestCalculated",
    "IsAccountClosed",
    "AccClassCode",
    "AccClass",
    "CheckKey",
    "ClientBK",
    "ClientID",
    "PackageCode",
    "ProductCode",
    "OvrDraftExceedDate1",
    "LastAccAgenceCode",
    "LastAccAgence",
    "LastAccCurrCode",
    "LastAccCurr",
    "CurrencyCode",
    "Currency",
    "CreditTurnover",
    "DebitTurnover",
    "LastCreditDate",
    "LastDebitDate",
    "LastMovementDate",
    "AccCloseDate",
    "MarkForCloseDate",
    "UpdateOn",
    "AccOpenDate",
    "IntInstCode",
    "IsClosurePending",
    "AccountTitle",
    "AccCloseReasonCode",
    "AccPledging",
    "AccountNumber",
    "ModifSheetNum",
    "IsAccTaxable",
    "AccountTypeCode",
    "AccountType",
    "InitiatedByCode",
    "InitiatedBy",
    "IndicativeBalance",
    "HistoryBalance",
    "CreatedByCode",
    "CreatedBy"
]

# Create Row_Hash column using SHA2_256
accountdim_hashed = accountdim_df.withColumn(
    "Row_Hash",
    sha2(concat_ws("", *[col(c).cast("string") for c in hash_columns]), 256)
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

# ### 4. Writing Data into "**SilverLakehouse.AccountDim**"

# CELL ********************

accountdim_hashed.write.mode("overwrite").saveAsTable("SilverLakehouse.CbsAccountDim")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
