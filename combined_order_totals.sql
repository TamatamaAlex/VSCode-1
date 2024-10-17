CREATE TABLE "combinedtables"."combined_order_totals" AS
SELECT 
    order_id, 
    tax_rate, 
    total, 
    tax, 
    total_incl_tax, 
    partition_0
FROM 
    "fulldataauroratos3"."ota_order_totals"

UNION ALL

SELECT 
    order_id, 
    tax_rate, 
    total, 
    tax, 
    total_incl_tax, 
    partition_0
FROM 
    "fulldataauroratos3"."learning_order_totals"

UNION ALL

SELECT 
    order_id, 
    tax_rate, 
    total, 
    tax, 
    total_incl_tax, 
    partition_0
FROM 
    "fulldataauroratos3"."azama_order_totals";