CREATE TABLE "combinedtables"."combined_conf" AS
SELECT 
    id, 
    name, 
    value, 
    serialize, 
    name_jp, 
    memo,
    partition_0
FROM 
    "auroratos3exporttest"."ota_conf"

UNION ALL

SELECT 
    id, 
    name, 
    value, 
    serialize, 
    name_jp, 
    memo,
    partition_0
FROM 
    "auroratos3exporttest"."learning_conf"

UNION ALL

SELECT 
    id, 
    name, 
    value, 
    serialize, 
    name_jp, 
    memo,
    partition_0
FROM 
    "auroratos3exporttest"."azama_conf";