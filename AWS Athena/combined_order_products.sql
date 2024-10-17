-- Date: 2024-10-16 Alex Day
-- This SQL script combines the data from three different tables into one table
CREATE TABLE "combinedtables"."combined_order_products" AS --rename the table to your own DB and whatever table name you want
SELECT 
    'ota' AS source, --this creates a column for the source of the data
    order_pro_id, --all of these columns are from the ota_order_products table. Change them to fit your own table's columns
    order_id,
    order_logistics_id,
    product_id,
    main_no,
    product_no,
    jan_code,
    location_no,
    product_name,
    set_id,
    set_name,
    unit_price,
    set_quantity,
    set_unit,
    order_pro_count,
    tax_rate,
    tax_type_id,
    tax_incl,
    item_type,
    shipping_size,
    set_custom1,
    set_custom2,
    set_custom3,
    pro_custom1,
    pro_custom2,
    pro_custom3,
    partition_0
FROM 
    "auroratos3exporttest"."ota_order_products" --insert your own DB and table name here

UNION ALL --this is the SQL command to combine the data from the other tables

SELECT 
    'learning' AS source,
    order_pro_id,
    order_id,
    order_logistics_id,
    product_id,
    main_no,
    product_no,
    jan_code,
    location_no,
    product_name,
    set_id,
    set_name,
    unit_price,
    set_quantity,
    set_unit,
    order_pro_count,
    tax_rate,
    tax_type_id,
    tax_incl,
    item_type,
    shipping_size,
    set_custom1,
    set_custom2,
    set_custom3,
    pro_custom1,
    pro_custom2,
    pro_custom3,
    partition_0
FROM 
    "auroratos3exporttest"."learning_order_products" --insert your own DB and table name here

UNION ALL

SELECT 
    'azama' AS source,
    order_pro_id,
    order_id,
    order_logistics_id,
    product_id,
    main_no,
    product_no,
    jan_code,
    location_no,
    product_name,
    set_id,
    set_name,
    unit_price,
    set_quantity,
    set_unit,
    order_pro_count,
    tax_rate,
    tax_type_id,
    tax_incl,
    item_type,
    shipping_size,
    set_custom1,
    set_custom2,
    set_custom3,
    pro_custom1,
    pro_custom2,
    pro_custom3,
    partition_0
FROM 
    "auroratos3exporttest"."azama_order_products"; --insert your own DB and table name here