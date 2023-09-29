SELECT 
cial_op_diff_log_msg_label,
count (distinct campaign_ID) as nb_campaign_ID,
count (distinct campaign_name) as nb_campaign_name
FROM `ard-corp-sc-sandbox.Valentin_ph_test.d_email_attribut` 
group by 1
having nb_campaign_ID > 1
