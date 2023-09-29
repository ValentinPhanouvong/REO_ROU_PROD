-- a_trigger_operation_marketing
-- Create or replace table `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_trigger_operation_marketing` as
---------
WITH diffusion_one_shot_domain_fai AS (
/*
Here we gather the email distribution data: 
        - Date of the sending attempt
        - Type of communication (here EMAIL)
        - Status of the sending : EMAIL SEND or EMAIL BOUNCE
        - If EMAIL BOUNCE : 
                - Reason for failure
                - Type of failure: EMAIL SOFT BOUNCE, EMAIL HARD BOUNCE, EMAIL BLOCK BOUNCE, EMAIL TECHNICAL BOUNCE or EMAIL UNKNOW BOUNCE
        - Unique customer ID
  
In diffusion_domain_fai, we use the same fields and add the ISP. 
The list of internet service providers has been defined by the French teams. 
Each country will have to define its own list of ISPs. 
*/

  SELECT
    message_id,
    cial_op_diff_log_channel_type_code,
    cial_op_diff_log_msg_status_code,
    cial_op_diff_log_msg_fail_reason_id,
    cial_op_diff_log_msg_fail_type_code,
    subscriberkey as subscriber_key,
    CASE
      WHEN domain IN ('aol.com') THEN 'AOL'
      WHEN domain IN ('auchan.ro', 'auchan.com', 'partner.auchan.ro', 'partner.auchan.com','auchan.ro','auchan.pl') THEN 'AUCHAN'
      WHEN domain IN ('gmail.com','gmail.com.com','gmail.com','googlemail.com','gmail.ro') THEN 'GMAIL'
      WHEN domain IN ('icloud.com') THEN 'Icloud'
      WHEN domain IN ('hotmail.com','outlook.com','mail.ru','live.com','hotmail.it','hotmail.fr','msn.com','windowslive.com','hotmail.es','hotmail.co.uk','outlook.fr','outlook.it','hotmail.de','outlook.de') THEN 'MICROSOFT'
      WHEN domain IN ('yahoo.ro','ymail.com','yahoo.co.uk','yahoo.it','yahoo.fr','yahoo.es','yahoo.ca','yahoo.de','rocketmail.com','yahoo.com.au','yahoo.ro','yahoo.ie','yahoo.com.sg','yahoo.co.nz','yahoo.co.in','yahoo.gr','yahoo.in','yahoo.CO.UK','yahoo.com.ph','yahoo.hu','yahoo.com.br','yahoo.com.mx','yahoo.se','yahoo.com.my','yahoo.com.tr','yahoo.com.ar','yahoo.DE') THEN 'YAHOO'
      ELSE 'AUTRE'
    END AS fai
  FROM `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_email_diffusion_operation_marketing`
  WHERE cial_op_diff_log_channel_type_code = 'EMAIL'
  group by 
  message_id,
    cial_op_diff_log_channel_type_code,
    cial_op_diff_log_msg_status_code,
    cial_op_diff_log_msg_fail_reason_id,
    cial_op_diff_log_msg_fail_type_code,
    subscriber_key,
    fai
), 



diffusion AS (
/*
In diffusion, we calculate the KPIs related to the diffusion : 
        - Date of the sending attempt
        - ISP
        - Volume of unique customers to whom an email was attempted
        - Volume of unique customers who received the email
        - Volume of unique customers who declared our mail as spam
        - Volume of unique customers with a soft bounce
        - Volume of unique customers with a hard bounce
        - Volume of unique customers with a block bounce
        - Volume of unique customers with a technical bounce
        - Volume of unique customers with an unknow bounce
        - Volume of unique customers with a bounce, regardless of the category
*/
  SELECT
    message_id,
    fai,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_channel_type_code='EMAIL' THEN subscriber_key ELSE NULL END)                                                                            AS emails,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_status_code='EMAIL SENT' THEN subscriber_key ELSE NULL END)
      - (COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_status_code='EMAIL BOUNCE' THEN subscriber_key ELSE NULL END))                                                                 AS received,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_fail_reason_id ='Complaints' THEN subscriber_key ELSE NULL END)                                                                     AS complaints,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_status_code='EMAIL BOUNCE' AND cial_op_diff_log_msg_fail_type_code='EMAIL SOFT BOUNCE' THEN subscriber_key ELSE NULL END)           AS soft,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_status_code='EMAIL BOUNCE' AND cial_op_diff_log_msg_fail_type_code='EMAIL HARD BOUNCE' THEN subscriber_key ELSE NULL END)           AS hard,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_status_code='EMAIL BOUNCE' AND cial_op_diff_log_msg_fail_type_code='EMAIL BLOCK BOUNCE' THEN subscriber_key ELSE NULL END)          AS block,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_status_code='EMAIL BOUNCE' AND cial_op_diff_log_msg_fail_type_code='EMAIL TECHNICAL BOUNCE' THEN subscriber_key ELSE NULL END)      AS technical,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_status_code='EMAIL BOUNCE' AND cial_op_diff_log_msg_fail_type_code='EMAIL UNKNOW BOUNCE' THEN subscriber_key ELSE NULL END)         AS unknown,
    COUNT(DISTINCT CASE WHEN cial_op_diff_log_msg_status_code='EMAIL BOUNCE' THEN subscriber_key ELSE NULL END)                                                                       AS bounce
  FROM diffusion_one_shot_domain_fai dif
  GROUP BY
      message_id,
      fai
),

tracking_domain_fai AS (
/*
Here we gather the email tracking data: 
        - Date of the sending attempt
        - Type of tracking event : 1 for opening, 2 for clics
        - If clic : 
                - URL category
        - Unique customer ID
In tracking_domain_fai, we use the same fields and add the ISP. 
The list of internet service providers has been defined by the French teams. 
Each country will have to define its own list of ISPs. 
*/
  SELECT
    message_id,
    cial_op_tracking_log_url_type_code,
    cial_op_tracking_log_url_category_code,
    subscriberkey as subscriber_key,
    CASE
      WHEN domain IN ('aol.com') THEN 'AOL'
      WHEN domain IN ('auchan.ro', 'auchan.com', 'partner.auchan.ro', 'partner.auchan.com','auchan.ro','auchan.pl') THEN 'AUCHAN'
      WHEN domain IN ('gmail.com','gmail.com.com','gmail.com','googlemail.com','gmail.ro') THEN 'GMAIL'
      WHEN domain IN ('icloud.com') THEN 'Icloud'
      WHEN domain IN ('hotmail.com','outlook.com','mail.ru','live.com','hotmail.it','hotmail.fr','msn.com','windowslive.com','hotmail.es','hotmail.co.uk','outlook.fr','outlook.it','hotmail.de','outlook.de') THEN 'MICROSOFT'
      WHEN domain IN ('yahoo.ro','ymail.com','yahoo.co.uk','yahoo.it','yahoo.fr','yahoo.es','yahoo.ca','yahoo.de','rocketmail.com','yahoo.com.au','yahoo.ro','yahoo.ie','yahoo.com.sg','yahoo.co.nz','yahoo.co.in','yahoo.gr','yahoo.in','yahoo.CO.UK','yahoo.com.ph','yahoo.hu','yahoo.com.br','yahoo.com.mx','yahoo.se','yahoo.com.my','yahoo.com.tr','yahoo.com.ar','yahoo.DE') THEN 'YAHOO'
      ELSE 'AUTRE'
    END AS fai
  FROM `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_email_tracking_operation_marketing`
  group by 
    message_id,
    cial_op_tracking_log_url_type_code,
    cial_op_tracking_log_url_category_code,
    subscriber_key,
    fai
),

tracking AS (
/*
In tracking, we calculate the KPIs related to the tracking : 
        - Date of the sending attempt
        - ISP
        - Volume of unique customers who opened the email at least once
        - Volume of unique customers who clicked on a link in the email at least once, other than the unsubscribe link
                (nb : The unsubscribe link is categorized as "G07%")
        - Volume of unique customers who clicked at least once on the unsubscribe link
                (nb : Here we only count the customers who clicked once on the unsubscribe link and not the actual unsubscribes)
*/
  SELECT
    message_id,
    fai,
    COUNT(DISTINCT CASE WHEN cial_op_tracking_log_url_type_code='1' THEN subscriber_key ELSE NULL END)                                                                    AS opened,
    COUNT(DISTINCT CASE WHEN cial_op_tracking_log_url_type_code='2' AND COALESCE(cial_op_tracking_log_url_category_code,"") NOT LIKE 'G07%' THEN subscriber_key ELSE NULL END)         AS clics,
    COUNT(DISTINCT CASE WHEN cial_op_tracking_log_url_type_code='2' AND cial_op_tracking_log_url_category_code LIKE 'G07%' THEN subscriber_key ELSE NULL END)             AS unsubscribed
  FROM tracking_domain_fai
  GROUP BY
      message_id,
      fai
),

routage_periode AS (
/*
In routage_periode, we have the start date and the end date of routing. 
*/

Select 
message_id,
min(start_date_routage) as start_date,
max(end_date_routage) as end_date
FROM `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.d_email_attributs`
where email_type = "Trigger"
group by
message_id
  
)


SELECT
Distinct
  routage_periode.message_id,
  routage_periode.start_date, -- define at message_id only
  routage_periode.end_date, -- define at  message_id only
  COALESCE(diffusion.fai, tracking_2.fai) as fai,
  diffusion.emails,
  diffusion.received,
  CASE WHEN diffusion.fai is not null then tracking.opened
  WHEN diffusion.fai is null and tracking.opened is null then tracking_2.opened 
  else -1 END as opened, -- allow us to display information if we have missing date inside the diffusion table / the else condition will help us to detect strange case 
  CASE WHEN diffusion.fai is not null then tracking.clics
  WHEN diffusion.fai is null and tracking.clics is null then tracking_2.clics 
  else -1 END as clics, -- allow us to display information if we have missing date inside the diffusion table / the else condition will help us to detect strange case 
  CASE WHEN diffusion.fai is not null then tracking.unsubscribed
  WHEN diffusion.fai is null and tracking.unsubscribed is null then tracking_2.unsubscribed 
  else -1 END as unsubscribed, -- allow us to display information if we have missing date inside the diffusion table / the else condition will help us to detect strange case  diffusion.complaints,
  diffusion.soft,
  diffusion.hard,
  diffusion.block,
  diffusion.technical,
  diffusion.unknown,
  diffusion.bounce
FROM
  routage_periode
LEFT JOIN diffusion
  on routage_periode.message_id = diffusion.message_id
LEFT JOIN tracking
  on diffusion.fai = tracking.fai and diffusion.message_id = tracking.message_id
LEFT JOIN tracking as tracking_2
  on routage_periode.message_id = tracking_2.message_id 
  and diffusion.message_id is null  -- the last part of the join condition allow us to ensure the fact the left join will happen only if the first left join tracking faill.
