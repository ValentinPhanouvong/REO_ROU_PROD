WITH rules_test as (

SELECT DISTINCT 
    subject,
    IF(CONTAINS_SUBSTR(subject, "[TEST"), "test", "no test") as colonne_test1,
    IF("Test" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test2,
    IF("test" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test3,
    IF("TEST" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test4,
    IF("TTest" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test5,
    IF("Template" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test6,
    IF("template" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test7,
    IF("TEMPLATE" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test8
  
  FROM
    `auchan-rou-prod.raw_salesforce_customeractivation_sec.email_send_log` email
  WHERE 1=1 
  --hors BAT
  --AND COALESCE(countrycustomerid,"") NOT LIKE "%ROU%"
  --hors test
AND LOWER(COALESCE(subject,"")) LIKE "%test%"
OR LOWER(COALESCE(subject,"")) LIKE "%template%"
)

SELECT 
subject,
rules_test.colonne_test1,
rules_test.colonne_test2,
rules_test.colonne_test3,
rules_test.colonne_test4,
rules_test.colonne_test5,
rules_test.colonne_test6,
rules_test.colonne_test7,
rules_test.colonne_test8,
CASE WHEN colonne_test1 = "no test" 
AND colonne_test2 = "no test"
AND colonne_test3 = "no test"
AND colonne_test4 = "no test"
AND colonne_test5 = "no test"
AND colonne_test6 = "no test"
AND colonne_test7 = "no test"
AND colonne_test8 = "no test"
THEN 'True'
ELSE 'False'
END AS detect_test
FROM rules_test
