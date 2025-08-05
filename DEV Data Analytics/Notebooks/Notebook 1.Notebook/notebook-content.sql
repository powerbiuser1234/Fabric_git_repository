-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "96e0ca87-80f3-4bec-92b1-b456099d09be",
-- META       "default_lakehouse_name": "RBK_Lakehouse",
-- META       "default_lakehouse_workspace_id": "d1ef06f0-6f40-4597-b69c-c056f928168c",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "f25322d6-67e7-4494-bac9-5062093fbc49"
-- META         },
-- META         {
-- META           "id": "44b8f0b4-6079-41b0-9290-1c678df72717"
-- META         },
-- META         {
-- META           "id": "96e0ca87-80f3-4bec-92b1-b456099d09be"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT TOP (100) [DCO],
            [CLI]
FROM [Lakehouse].[dbo].[VW_CR_MVT_RB_FINAL]

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
