
SELECT DISTINCT 
    subject,
    IF(CONTAINS_SUBSTR(subject, "[test"), "test", "no test") as colonne_test,
    IF(CONTAINS_SUBSTR(subject, "[TEMPLATE"), "test", "no test") as colonne_test2,
    IF("Template" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test_template,
    IF("template" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test_template2,
    IF("TEMPLATE" in UNNEST(SPLIT(REGEXP_REPLACE((subject), r'[^a-zA-Z]', ' '), " ")), "test", "no test") as colonne_test_template3

  FROM
    `auchan-rou-prod.raw_salesforce_customeractivation_sec.email_send_log` email
  WHERE 1=1 
  --hors BAT
  --AND COALESCE(countrycustomerid,"") NOT LIKE "%ROU%"
  --hors test
  AND LOWER(COALESCE(subject,"")) LIKE "%test%"
  OR LOWER(COALESCE(subject,"")) LIKE "%template%"
