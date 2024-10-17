-- Date: 2024-10-16 Alex Day
-- This SQL script combines the data from three different tables into one table
CREATE TABLE "combinedtables"."combined_order_totals" AS --rename the table to your own DB and whatever table name you want
SELECT 
    'ota' AS source, --this creates a column for the source of the data
    order_id, --all of these columns are from the ota_order_totals table. Change them to fit your own table's columns
    tax_rate, 
    total, 
    tax, 
    total_incl_tax, 
    partition_0
FROM 
    "auroratos3exporttest"."ota_order_totals" --insert your own DB and table name here

UNION ALL --this is the SQL command to combine the data from the other tables

SELECT
    'learning' AS source,
    order_id, 
    tax_rate, 
    total, 
    tax, 
    total_incl_tax, 
    partition_0
FROM 
    "auroratos3exporttest"."learning_order_totals" --insert your own DB and table name here

UNION ALL

SELECT
    'azama' AS source,
    order_id, 
    tax_rate, 
    total, 
    tax, 
    total_incl_tax, 
    partition_0
FROM 
    "auroratos3exporttest"."azama_order_totals"; --insert your own DB and table name here