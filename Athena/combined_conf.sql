-- Date: 2024-10-17 Alex Day
-- This SQL script combines the data from three different tables into one table
CREATE TABLE "combinedtables"."combined_conf" AS --rename the table to your own DB and whatever table name you want
SELECT 
    'ota' AS source, --this creates a column for the source of the data
    id, --all of these columns are from the ota_conf table. Change them to fit your own table's columns
    name, 
    value, 
    serialize, 
    name_jp, 
    memo,
    partition_0
FROM 
    "auroratos3exporttest"."ota_conf" --insert your own DB and table name here

UNION ALL --this is the SQL command to combine the data from the other tables

SELECT 
    'learning' AS source,
    id, 
    name, 
    value, 
    serialize, 
    name_jp, 
    memo,
    partition_0
FROM 
    "auroratos3exporttest"."learning_conf" --insert your own DB and table name here

UNION ALL

SELECT 
    'azama' AS source,
    id, 
    name, 
    value, 
    serialize, 
    name_jp, 
    memo,
    partition_0
FROM 
    "auroratos3exporttest"."azama_conf"; --insert your own DB and table name here