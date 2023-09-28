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
    email_tracking.subscriberkey,
    subject,
    IF(CONTAINS_SUBSTR(subject, "[TEST"), "il y a test", "il n'y a pas test") as colonne_test
  FROM
    `auchan-rou-prod.raw_salesforce_customeractivation_sec.email_tracking_log` email_tracking
  WHERE true 
  --hors BAT
  AND COALESCE(countrycustomerid,"") NOT LIKE "%ROU%"
  --hors test
    AND LOWER(COALESCE(subject,"")) NOT LIKE "%template%" )

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
    subject,
    colonne_test,
    CONCAT("RO", "_SF_", CountryCustomerId) AS person_unique_code,
    subscriberkey
  FROM
    dl_mapped dl
Where colonne_test = "il n'y a pas test"
