-- a_email_diffusion_operation_marketing

--Create or replace table `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_email_diffusion_operation_marketing` as
-----
WITH
  dl_mapped AS (
  SELECT
    lower(email.Address) as Address,
    email.ContactDate,
    email.EventDate,
    email.Reason,
    email.CountryCustomerId,
    email.subscriberkey,
    email.messageid,
    trim(email.label) as label,
    CASE status
      WHEN 1 THEN 'EMAIL SENT'
      WHEN 2 THEN 'EMAIL BOUNCE'
      ELSE ''
    END AS cial_op_diff_log_msg_status_code,
    CASE FailureType
      WHEN 1 THEN 'EMAIL SOFT BOUNCE'
      WHEN 2 THEN 'EMAIL HARD BOUNCE'
      WHEN 3 THEN 'EMAIL BLOCK BOUNCE'
      WHEN 4 THEN 'EMAIL UNKNOW BOUNCE'
      WHEN 5 THEN 'EMAIL TECHNICAL BOUNCE'
      ELSE ''
    END AS cial_op_diff_log_msg_fail_type_code,
    "EMAIL" AS cial_op_diff_log_channel_type_code,
    subject,
    IF(CONTAINS_SUBSTR(subject, "[TEST"), "il y a test", "il n'y a pas test") as colonne_test
  FROM
    `auchan-rou-prod.raw_salesforce_customeractivation_sec.email_send_log` email
  WHERE 1=1 
  --hors BAT
  AND COALESCE(countrycustomerid,"") NOT LIKE "%ROU%"
  --hors test
    AND LOWER(COALESCE(subject,"")) NOT LIKE "%template%")
  -- manage temporality
   -- and tech_date between date_sub(current_date(), interval 7 DAY) and current_date()) -- tech_date = date(eventdate) made by IT when they load the data

SELECT
DISTINCT
  COALESCE(label,"No label") AS cial_op_diff_log_msg_label,
  subject,
  colonne_test,
  dl.cial_op_diff_log_channel_type_code AS cial_op_diff_log_channel_type_code,
  SUBSTR(Address, strpos(Address, '@')+1) AS domain,
  CAST(ContactDate AS DATETIME) AS cial_op_diff_log_contact_date,
  CAST(EventDate AS DATETIME) AS cial_op_diff_log_event_date,
  dl.cial_op_diff_log_msg_status_code AS cial_op_diff_log_msg_status_code,
  dl.cial_op_diff_log_msg_fail_type_code AS cial_op_diff_log_msg_fail_type_code,
  Reason AS cial_op_diff_log_msg_fail_reason_id,
  "SF" AS data_src_code,
  CURRENT_DATETIME() AS cre_date,
  CURRENT_DATETIME() AS upd_date,
  "RO" AS ctry_code,
  CONCAT("RO", "_SF_", CountryCustomerId) AS person_unique_code,
  subscriberkey,
  dl.messageid as message_id
FROM
dl_mapped dl
Where colonne_test = "il n'y a pas test"
