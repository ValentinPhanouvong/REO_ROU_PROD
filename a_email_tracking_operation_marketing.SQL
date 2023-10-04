-- a_email_tracking_operation_marketing
--Create or replace table `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_email_tracking_operation_marketing` as
-----------

WITH
  dl_mapped AS (
  SELECT
    email_tracking.messageid,
    email_tracking.EventDate,
    email_tracking.ContactDate,
    email_tracking.TYPE,
    email_tracking.UrlId,
    email_tracking.Category,
    email_tracking.SourceURL, 
    email_tracking.Device, 
    email_tracking.OperatingSystem, 
    email_tracking.Browser, 
    lower(email_tracking.Address) as Address,
    email_tracking.jobid, 
    trim(email_tracking.label) as label,
    email_tracking.CountryCustomerId,
    email_tracking.subscriberkey
  FROM
    `auchan-rou-prod.raw_salesforce_customeractivation_sec.email_tracking_log` email_tracking
WHERE 1=1 
  -- Hors BAT
  AND COALESCE(countrycustomerid, "") NOT LIKE "%ROU%"
  -- Hors test
  AND (
    (
      trim(subject) IN ("💡1+1, 2+1, 3+1 🧐 Nu e un test de matematica, sunt noile oferte ▶️") -- list of labels that we want to keep
    )
    OR (
      (
        (IF(CONTAINS_SUBSTR(subject, "[TEST"), "test", "no test") = "no test") -- we want to delete all the [Test...
        AND (IF("Test" in UNNEST(SPLIT(REGEXP_REPLACE(subject, r'[^a-zA-Z]', ' '), " ")), "test", "no test") = "no test")-- we want to delete all the Test
        AND (IF("test" in UNNEST(SPLIT(REGEXP_REPLACE(subject, r'[^a-zA-Z]', ' '), " ")), "test", "no test") = "no test")-- we want to delete all the test
        AND (IF("TEST" in UNNEST(SPLIT(REGEXP_REPLACE(subject, r'[^a-zA-Z]', ' '), " ")), "test", "no test") = "no test")-- we want to delete all the TEST
        AND (IF("TTest" in UNNEST(SPLIT(REGEXP_REPLACE(subject, r'[^a-zA-Z]', ' '), " ")), "test", "no test") = "no test")-- we want to delete all the TTest
        AND (IF(CONTAINS_SUBSTR(subject, "[TEMPLATE"), "test", "no test") = "no test") -- we want to delete all the [TEMPLATE...
        AND (IF("Template" in UNNEST(SPLIT(REGEXP_REPLACE(subject, r'[^a-zA-Z]', ' '), " ")), "test", "no test") = "no test")-- we want to delete all the Template
        AND (IF("template" in UNNEST(SPLIT(REGEXP_REPLACE(subject, r'[^a-zA-Z]', ' '), " ")), "test", "no test") = "no test")-- we want to delete all the template
        AND (IF("TEMPLATE" in UNNEST(SPLIT(REGEXP_REPLACE(subject, r'[^a-zA-Z]', ' '), " ")), "test", "no test") = "no test")
        -- we want to delete all the "TEMPLATE"
      )
      -- manage temporality
      -- and tech_date between date_sub(current_date(), interval 7 DAY) and current_date()) -- tech_date = date(eventdate) made by IT when they load the data
    )
  )
   )

  SELECT
  distinct
    CAST(EventDate AS DATETIME) AS cial_op_tracking_log_event_date,
    CAST(ContactDate AS DATETIME) AS cial_op_tracking_log_contact_date,
    CAST(TYPE AS STRING) AS cial_op_tracking_log_url_type_code,
    Category AS cial_op_tracking_log_url_category_code,
    "SF" AS data_src_code,
    CURRENT_DATETIME() AS cre_date,
    CURRENT_DATETIME() AS upd_date,
    "RO" AS ctry_code,
    dl.messageid as message_id,
     SUBSTR(Address, strpos(Address, '@')+1) AS domain,
    COALESCE(label,"no label") AS cial_op_diff_log_msg_label,
    CONCAT("RO", "_SF_", CountryCustomerId) AS person_unique_code,
    subscriberkey
  FROM
    dl_mapped dl
