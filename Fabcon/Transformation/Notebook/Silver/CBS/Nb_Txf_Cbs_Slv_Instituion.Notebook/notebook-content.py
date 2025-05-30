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

# #### **2. Reading Tables**

# CELL ********************

bkbqe = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkbqe")
cms_bank=spark.read.format("delta").table("BronzeLakehouse.CMS_Bank")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3. Institution ETL**

# MARKDOWN ********************

# ###### **3.1 BKBQE DF**

# CELL ********************

bkbqe_df = (
    bkbqe
    .select(
        trim(bkbqe.AGE).alias("BranchCode"),
        trim(bkbqe.ETAB).alias("BankCode"),
        trim(bkbqe.GUIB).alias("CounterCode"),
        trim(bkbqe.NOM).alias("BankName"),
        trim(bkbqe.DOMI).alias("BankDomicile"),
        trim(bkbqe.CPOS).alias("Postcode"),
        trim(bkbqe.AGER).alias("LinkedBranchCode"),
        trim(bkbqe.DEV).alias("CurrencyCode"),
        trim(bkbqe.SPHP).alias("TownBankType"),
        trim(bkbqe.CPLA).alias("BankingCentreCode"),
        bkbqe.COMP.alias("Clearable"),
        bkbqe.NBJE.alias("CashingDays"),
        bkbqe.NBJR.alias("BillsRecoveryDays"),
        bkbqe.NBJRFM.alias("MonthEndCollectionDays"),
        bkbqe.NBJB.alias("BankingDays"),
        bkbqe.NJEF.alias("BillsCollectionDays"),
        trim(bkbqe.PAYS).alias("CountryCode"),
        trim(bkbqe.ETAP).alias("MainLinkedBankCode"),
        trim(bkbqe.GUIP).alias("MainLinkedCounterCode"),
        trim(bkbqe.TSEM).alias("WeekType"),
        trim(bkbqe.SWIFT).alias("SwiftCode"),
        trim(bkbqe.PLB).alias("BankingCentre"),
        bkbqe.NJEAC.alias("DispatchPeriodDays"),
        bkbqe.NJRAC.alias("AcceptanceReturnDays"),
        trim(bkbqe.CVILLE).alias("TownCode"),
        trim(bkbqe.CVCOMP).alias("ClearingTownCode"),
        trim(bkbqe.CRDV).alias("MoroccoArea"),
        trim(bkbqe.NOMET).alias("InstitutionName"),
        trim(bkbqe.BIC).alias("CentralBankID"),
        bkbqe.DOU.alias("CreationDate"),
        bkbqe.DMO.alias("LastModified"),
        trim(bkbqe.UTI).alias("ModifiedBy"),
        bkbqe.BatchID.alias("Batch_ID"),
        bkbqe.BATCHDATE.alias("Batch_Date"),
        to_date(bkbqe.BATCHDATE).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        bkbqe.SystemCode.alias("System_Code"),
        lit(None).cast("binary").alias("Row_Hash"),
        bkbqe.WorkFlowName.alias("Workflow_Name")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **3.2 CMS_Bank DF**

# CELL ********************

cms_bank_df = (
    cms_bank
    .select(
        lit("").alias("BranchCode"),
        cms_bank.BAN_CODE.alias("BankCode"),
        cms_bank.BAN_IDEN.alias("CounterCode"),
        cms_bank.BAN_CORP_NAME.alias("BankName"),
        lit(None).cast("string").alias("BankDomicile"),
        lit(None).cast("string").alias("Postcode"),
        lit(None).cast("string").alias("LinkedBranchCode"),
        cms_bank.BAN_ACQU_CURR_CODE.alias("CurrencyCode"),
        lit(None).cast("string").alias("TownBankType"),
        lit(None).cast("string").alias("BankingCentreCode"),
        lit(None).cast("int").alias("Clearable"),
        lit(None).cast("int").alias("CashingDays"),
        lit(None).cast("int").alias("BillsRecoveryDays"),
        lit(None).cast("int").alias("MonthEndCollectionDays"),
        lit(None).cast("int").alias("BankingDays"),
        lit(None).cast("int").alias("BillsCollectionDays"),
        lit(None).cast("string").alias("CountryCode"),
        lit(None).cast("string").alias("MainLinkedBankCode"),
        lit(None).cast("string").alias("MainLinkedCounterCode"),
        lit(None).cast("string").alias("WeekType"),
        lit(None).cast("string").alias("SwiftCode"),
        lit(None).cast("string").alias("BankingCentre"),
        lit(None).cast("int").alias("DispatchPeriodDays"),
        lit(None).cast("int").alias("AcceptanceReturnDays"),
        lit(None).cast("string").alias("TownCode"),
        lit(None).cast("string").alias("ClearingTownCode"),
        lit(None).cast("string").alias("MoroccoArea"),
        lit(None).cast("string").alias("InstitutionName"),
        lit(None).cast("string").alias("CentralBankID"),
        lit(None).cast("timestamp").alias("CreationDate"),
        lit(None).cast("timestamp").alias("LastModified"),
        lit(None).cast("string").alias("ModifiedBy"),
        cms_bank.BATCHID.alias("Batch_ID"),
        cms_bank.BATCHDATE.alias("Batch_Date"),
        to_date(cms_bank.BATCHDATE).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        cms_bank.SYSTEMCODE.alias("System_Code"),
        lit(None).cast("binary").alias("Row_Hash"),
        cms_bank.WORKFLOWNAME.alias("Workflow_Name")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **3.3 Adding Values into RawHAsh**

# CELL ********************

institution_df = bkbqe_df.union(cms_bank_df)

institution_df = institution_df.withColumn(
    "Row_Hash",
    sha2(concat_ws("",
        "BranchCode", "BankCode", "CounterCode", "BankName", "BankDomicile", "Postcode",
        "LinkedBranchCode", "CurrencyCode", "TownBankType", "BankingCentreCode", "Clearable",
        "CashingDays", "BillsRecoveryDays", "MonthEndCollectionDays", "BankingDays",
        "BillsCollectionDays", "CountryCode", "MainLinkedBankCode", "MainLinkedCounterCode",
        "WeekType", "SwiftCode", "BankingCentre", "DispatchPeriodDays", "AcceptanceReturnDays",
        "TownCode", "ClearingTownCode", "MoroccoArea", "InstitutionName", "CentralBankID",
        "CreationDate", "LastModified", "ModifiedBy"
    ), 256).cast("Binary")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  #### **4. Writing Data into Silver.Cbs_Instituion**

# CELL ********************

institution_df.write.format("delta").mode("append").saveAsTable("SilverLakehouse.Cbs_Institution")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
