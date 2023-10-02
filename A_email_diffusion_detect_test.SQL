SELECT DISTINCT 
    subject,
    IF("Test" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test1,
    IF("test" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test2,
    IF("TEST" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test3,
    IF("Template" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test4,
    IF("template" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test5,
    IF("TEMPLATE" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test6,
  FROM
    `auchan-rou-prod.raw_salesforce_customeractivation_sec.email_send_log` email
  WHERE 1=1 
  --hors BAT
  --AND COALESCE(countrycustomerid,"") NOT LIKE "%ROU%"
  --hors test
AND LOWER(COALESCE(subject,"")) LIKE "%test%"
OR LOWER(COALESCE(subject,"")) LIKE "%template%"
