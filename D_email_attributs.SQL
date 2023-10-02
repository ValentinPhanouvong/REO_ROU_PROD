-- d_email_attributs
-- Create or replace table `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.d_email_attributs` as

/* 

the goal of this table is to have only one email attribut per message_id & contact date. 

this is possible for email label
For campaign attributs (name, code and message frequency) as we don't have the contact date detail we put some security to ensure the unicity of the following element :

a message id can have only one campaign and one message frequency and this campaign should have one name and one code

*/
--------------
with 

/*1er bloc : Avoir les informations de routage des mails (min et max) par messageID, contact_date */
email_diff_routage as (
SELECT  
message_id, 
date(cial_op_diff_log_contact_date) as cial_op_diff_log_contact_date , 
min(cial_op_diff_log_event_date) as start_date_routage,
max(cial_op_diff_log_event_date) as end_date_routage
FROM `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_email_diffusion_operation_marketing` 
group by 
message_id,  
cial_op_diff_log_contact_date ),

/*2ème bloc : Remonter le label le plus récent par messageID,label,contact_date en prenant le max de la event_date --> Max de la date à laquelle le client a reçu ou bounce sur la table de diffusion*/
email_diff_label_list as (
SELECT  
message_id, 
cial_op_diff_log_msg_label, 
date(cial_op_diff_log_contact_date) as cial_op_diff_log_contact_date , 
max(cial_op_diff_log_event_date) as last_log_available
FROM `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_email_diffusion_operation_marketing` 
group by 
message_id, 
cial_op_diff_log_msg_label, 
cial_op_diff_log_contact_date ),

/*3ème bloc : Remonter le label le plus récent par messageID,label,contact_date en prenant le max de la event_date --> Max de la date à laquelle le client à ouvert ou cliqué sur le mail sur la table de la tracking */
email_tracking_label_list as ( 

SELECT 
message_id,
cial_op_diff_log_msg_label, 
date(cial_op_tracking_log_contact_date) as cial_op_tracking_log_contact_date, 
max(cial_op_tracking_log_event_date) as last_log_available
FROM `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_email_tracking_operation_marketing`
group by
message_id, 
cial_op_diff_log_msg_label, 
cial_op_tracking_log_contact_date


),

/*4ème bloc : Faire un union ALL de toutes les possibilités que l'on puisse avoir sur les 2 tables */
email_full_label_list as (
select *
from email_diff_label_list
union all 
select * 
from email_tracking_label_list
),

/*5ème bloc : 
Ce bloc va nous permettre d'avoir les combinaisons possible de messageID,label,contact_date,last_log_available en les numérotant grâce à la fonction row_number */
last_label_detection as (
select 
message_id, 
cial_op_diff_log_msg_label,
cial_op_diff_log_contact_date,
last_log_available,
row_number() OVER(partition BY message_id, cial_op_diff_log_contact_date order by last_log_available desc) as last_row
from (
    Select 
    message_id, 
    cial_op_diff_log_msg_label, 
    cial_op_diff_log_contact_date,
    max(last_log_available) as last_log_available,
    from email_full_label_list
    group by 
    message_id, 
    cial_op_diff_log_msg_label, 
    cial_op_diff_log_contact_date 
    )
),

/*6ème bloc : Ce  bloc va permettre de ramener le label le plus récent en filtrant sur le last_row = 1 */
last_label_detection_clean as (

Select 
Distinct 
message_id,
cial_op_diff_log_contact_date,
cial_op_diff_log_msg_label,
from last_label_detection
where  last_row = 1

),

/*7ème bloc : Ce bloc permet de prendre la cmp_id la plus récente en utilisant le row_number (order by cmp_update_desc) et en filtrant sur le lastr_ow = 1 */
campaign_clean as (

select distinct cmp_id, cmp_code, cmp_name
from (
SELECT  cmp_id, cmp_code, cmp_name, 
row_number() OVER(partition BY cmp_id order by cmp_update_date desc) as last_row -- take the most recent information into account
from  `auchan-rou-prod.raw_salesforce_customeractivation_sec.cco_campaign` )
where last_row = 1 

),

/*8ème bloc : Ce bloc va permettre de remonter les infos de fréquence(one_shot/trigger),msg_id et cmp_id de la table de la cco_campaign_messages en filtrant sur les mails*/
campaign_message as (

select distinct cmp_id, msg_id, msg_frequency, msg_content_type
FROM `auchan-rou-prod.raw_salesforce_customeractivation_sec.cco_campaign_messages`
where msg_channel = 'EMAIL'

),

/*9ème bloc : Jointure des blocs 7-8 qui vont permettre d'avoir les informations de cmp_id,campagneID,campagnename,messageID,frequence du message*/
campaign_message_full as 
(

select 
distinct
campaign_message.msg_id as message_id,
campaign_message.msg_frequency,
campaign_message.msg_content_type,
campaign_message.cmp_id,
campaign_clean.cmp_code AS campaign_id,
campaign_clean.cmp_name AS campaign_name
from campaign_message 
left join  campaign_clean
using(cmp_id) 
),

/*10ème bloc : A partir de la table last_label_detection_clean, on apporte les informations de la table campaign_message_full puis les informations de la table de routage en ajoutant les condition pour classifier en one-shot/trigger les différents messageID */
all_label_list as (
Select 
Distinct
message_full.message_id,
message_full.cial_op_diff_log_contact_date,
message_full.cial_op_diff_log_msg_label,
campaign_message_full.msg_frequency,
campaign_message_full.msg_content_type,
email_diff_routage.start_date_routage,
email_diff_routage.end_date_routage,
CASE 
WHEN (campaign_message_full.msg_frequency = 'One-shot' and UPPER(COALESCE(cial_op_diff_log_msg_label,"")) NOT LIKE '%TRIGGER%') 
            or (campaign_message_full.msg_frequency IS NULL and UPPER(COALESCE(cial_op_diff_log_msg_label,"")) NOT LIKE '%TRIGGER%') THEN 'One-shot' 
WHEN  (campaign_message_full.msg_frequency = 'Trigger') 
            or (campaign_message_full.msg_frequency IS NULL and UPPER(cial_op_diff_log_msg_label) LIKE '%TRIGGER%') 
            or (UPPER(cial_op_diff_log_msg_label) LIKE '%TRIGGER%') THEN 'Trigger' -- this last condition is specific for Roumania
ELSE 'Not defined' END  as email_type,
COALESCE(campaign_message_full.campaign_id,"no cmp_code") AS campaign_id,
COALESCE(campaign_message_full.campaign_name, "no_cmp_name") AS campaign_name
from last_label_detection_clean as message_full
left join campaign_message_full as campaign_message_full
on message_full.message_id = campaign_message_full.message_id
left join email_diff_routage as email_diff_routage
on email_diff_routage.message_id = message_full.message_id 
and email_diff_routage.cial_op_diff_log_contact_date = message_full.cial_op_diff_log_contact_date
),


Date_max as (
SELECT 
message_id,
cial_op_diff_log_contact_date,
cial_op_diff_log_msg_label,
msg_frequency,
msg_content_type,
start_date_routage,
end_date_routage,
email_type,
campaign_id,
campaign_name,
row_number()
      OVER (PARTITION BY cial_op_diff_log_msg_label ORDER BY cial_op_diff_log_contact_date DESC) as flag
FROM all_label_list
)

select 
message_id,
cial_op_diff_log_contact_date,
cial_op_diff_log_msg_label,
msg_frequency,
msg_content_type,
start_date_routage,
end_date_routage,
email_type,
campaign_id,
campaign_name,
flag,
from date_max
WHERE flag = 1
