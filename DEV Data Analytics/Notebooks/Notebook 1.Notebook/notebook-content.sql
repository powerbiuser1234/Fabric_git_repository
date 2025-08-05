-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "e56ddaf5-2e15-4634-91cc-e2eb818afa61",
-- META       "default_lakehouse_name": "Lakehouse",
-- META       "default_lakehouse_workspace_id": "f1bde452-2399-41bc-9ba5-b3c078a190fc",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "f25322d6-67e7-4494-bac9-5062093fbc49"
-- META         },
-- META         {
-- META           "id": "44b8f0b4-6079-41b0-9290-1c678df72717"
-- META         },
-- META         {
-- META           "id": "96e0ca87-80f3-4bec-92b1-b456099d09be"
-- META         },
-- META         {
-- META           "id": "e56ddaf5-2e15-4634-91cc-e2eb818afa61"
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

select CLI,NCP,AGE,DEV,SDE FROM Lakehouse.Cbs_Bkcom

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
