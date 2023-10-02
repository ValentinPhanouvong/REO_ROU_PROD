--  a_campaign_operation_marketing
--CREATE OR REPLACE TABLE `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_campaign_operation_marketing` AS 

SELECT DISTINCT
  CONCAT(EXTRACT(MONTH FROM CAST(Diffusion.cial_op_diff_log_contact_date AS date)),"_", EXTRACT(YEAR FROM CAST(Diffusion.cial_op_diff_log_contact_date AS date))) AS DateEnvois,
  COUNT(DISTINCT CampagneID) as Nbr_campaign, 
  COUNT(DISTINCT Message_ID) as Nbr_emails
FROM 
  `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.a_email_diffusion_operation_marketing` Diffusion
left join `ard-corp-sc-sandbox.CPA_ROU_TB_TEST.d_email_attributs`
using (message_ID)
WHERE 1=1
  AND Diffusion.cial_op_diff_log_channel_type_code LIKE 'EMAIL'
GROUP BY
  DateEnvois
