DECLARE from_date DATE DEFAULT '2024-05-01'; DECLARE to_date DATE DEFAULT '2024-05-27';

CREATE OR REPLACE TABLE `peya-delivery-and-support.user_rodrigo_demarco.customer_contacts` AS 

SELECT 
  created_date_Mvd,
  contact_id,
  service,
  vertical,
  CR1,
  CR2,
  CR3,
  CR4,
  category,
  interval_start_at,
  order_id,
  visitor_id,
  queue_name,
  lob,
  att,
  acw,
  ag_is_expert,
  sat_value,
  is_transf_chat_ind,
  is_sla30_fa_ind,
  is_missed_ind,


FROM `peya-delivery-and-support.automated_tables_reports.ops_contacts`
WHERE created_date_Mvd BETWEEN from_date AND to_date AND service = 'Customer'