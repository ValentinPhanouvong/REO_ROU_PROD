WITH
/*Ce bloc permet de prendre la cmp_id la plus récente en utilisant le
row_number (order by cmp_update_desc) et en filtrant sur le lastr_ow = 1 */
campaign_clean AS (

  SELECT DISTINCT
    cmp_id,
    cmp_code,
    cmp_name
  FROM (
    SELECT
      cmp_id,
      cmp_code,
      cmp_name,
      -- take the most recent information into account
      row_number() OVER (PARTITION BY cmp_id ORDER BY cmp_update_date DESC) AS last_row
    FROM `auchan-rou-prod.raw_salesforce_customeractivation_sec.cco_campaign`
  )
  WHERE last_row = 1

),

/*Ce bloc va permettre de remonter les infos de fréquence(one_shot/trigger),msg_id et
cmp_id de la table de la cco_campaign_messages en filtrant sur les mails*/
campaign_message AS (

  SELECT DISTINCT
    cmp_id,
    msg_id,
    msg_frequency,
    msg_content_type
  FROM `auchan-rou-prod.raw_salesforce_customeractivation_sec.cco_campaign_messages`
  WHERE msg_channel = 'EMAIL'

)

/*Jointure des blocs précédents qui vont permettre d'avoir les informations de
cmp_id,campagneID,campagnename,messageID,frequence du message*/

SELECT DISTINCT
  campaign_message.msg_id AS message_id,
  campaign_message.msg_frequency,
  campaign_message.msg_content_type,
  campaign_message.cmp_id,
  coalesce(campaign_clean.cmp_code, 'no cmp_code') AS campagneid,
  coalesce(campaign_clean.cmp_name, 'no_cmp_name') AS campagnename
FROM campaign_message
LEFT JOIN campaign_clean
  USING (cmp_id)
