
DECLARE from_date DATE DEFAULT '2024-07-01'; DECLARE to_date DATE DEFAULT '2025-01-27';

CREATE OR REPLACE TABLE `peya-delivery-and-support.automated_tables_reports.cusops_ovperformance`
PARTITION BY created_date
CLUSTER BY country_name, CCR1, CCR2

AS

WITH orders AS ( 
SELECT 
DATE(o.registered_at_utc) AS created_date,
o.country.country_code AS country_name, 
o.business_type.business_type_name AS vertical,
o.delivery_type,
  CASE WHEN p.is_darkstore = true THEN "Yes"
  ELSE "No"
  END AS isDmart,
"Order related" AS order_related,
"NULL" AS live_contact,
"NULL" AS CCR1,
"NULL" AS CCR2,
"NULL" AS CCR3,
--"Without CCR1_TW" AS CCR1_tweety,
--"Without CCR2_TW" AS CCR2_tweety,
--"Without CCR3_TW" AS CCR3_tweety,
COUNT(order_id) AS orders,
NULL AS sessions,
NULL AS contacts,
NULL AS csat_positive_response_all,
NULL AS csat_response_all,
NULL AS csat_response_ac,
NULL AS csat_response_ssf,
NULL AS csat_positive_response_ac,
NULL AS csat_positive_response_ssf,
NULL AS csat_triggered_ssf,
--NULL AS Retyp,
NULL AS Fcr_contacts,
NULL AS Fcr,
NULL AS Recontact
FROM `peya-bi-tools-pro.il_core.fact_orders` o
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON p.partner_id = o.restaurant.id
WHERE o.registered_date BETWEEN from_date -1 AND to_date+1 AND DATE(o.registered_at_utc) BETWEEN from_date AND to_date
GROUP BY 1,2,3,4,5,6,7,8
),

sessions AS (
SELECT 
created_date,
country_name, 
vertical,
delivery_type,
isDmart,
CASE WHEN order_id IS NOT NULL THEN "Order related" else 'Non order related' END AS order_related, 
CASE WHEN CCR1 = 'Live Order Process' THEN 'Live Contact' ELSE 'Non Live Contact' END AS live_contact,
CCR1, 
CCR2,
CCR3,
NULL AS orders,
COUNT(DISTINCT session_id) AS sessions,
NULL AS contacts,
NULL AS csat_positive_response_all,
NULL AS csat_response_all,
NULL AS csat_response_ac,
NULL AS csat_response_ssf,
NULL AS csat_positive_response_ac,
NULL AS csat_positive_response_ssf,
SUM(csat_trigger) AS csat_triggered_ssf,
NULL AS Fcr_contacts,
NULL AS Fcr,
NULL AS Recontact
FROM  `peya-delivery-and-support.automated_tables_reports.cusOps_session_level`  s
WHERE created_date BETWEEN from_date AND to_date
GROUP BY 1,2,3,4,5,6,7,8,9,10
),

csat AS (
SELECT 
c.created_date,
RIGHT(global_entity_id,2) AS country_name, 
vertical AS vertical, 
delivery_type AS delivery_type,
o.isDmart,
CASE WHEN ss.order_id IS NOT NULL THEN "Order related" else 'Non order related' END AS order_related, 
CASE WHEN gc.contact_reason_l1 = 'Live Order Process' THEN 'Live Contact' ELSE 'Non Live Contact' END AS live_contact, 
CASE WHEN gc.contact_reason_l1 IS NULL THEN 'NULL' ELSE gc.contact_reason_l1 END AS CCR1,
CASE WHEN gc.contact_reason_l2 IS NULL THEN 'NULL' ELSE gc.contact_reason_l2 END AS CCR2,
CASE WHEN gc.contact_reason_l3 IS NULL THEN 'NULL' ELSE gc.contact_reason_l3 END  AS CCR3,
NULL AS orders,
NULL AS sessions,
NULL AS contacts,
count( distinct case when csat > 3 then response_id else null end)  AS csat_positive_response_all,
count( distinct case when csat < 6 then response_id else null end) AS csat_response_all,
count(distinct case when csat < 6 AND survey_type = 'After Contact' THEN response_id ELSE NULL END) AS csat_response_ac,
count(distinct case when csat < 6 AND survey_type = 'Self Service' THEN response_id ELSE NULL END) AS csat_response_ssf,
COUNT(distinct case when csat > 3 AND survey_type = 'After Contact' THEN response_id ELSE NULL END) AS csat_positive_response_ac,
COUNT(distinct case when csat > 3 AND survey_type = 'Self Service' THEN response_id ELSE NULL END) AS csat_positive_response_ssf,
NULL AS csat_triggered_ssf,
NULL AS Fcr_contacts,
NULL AS Fcr,
NULL AS Recontact
FROM `peya-data-origins-pro.cl_gcc_service.tweety_csat_responses` c
LEFT JOIN `peya-delivery-and-support.automated_tables_reports.global_contact_reasons` AS GC ON global_ccr_code = global_cr_code AND contact_category = 'Customer'
LEFT JOIN  `peya-delivery-and-support.automated_tables_reports.cusOps_session_level` ss ON ss.session_id = c.session_id AND ss.created_date BETWEEN from_date -1 AND to_date+1
LEFT JOIN (
  SELECT 
order_id,
  CASE WHEN o.order_id IS NULL THEN 'No Order Related'
  WHEN p.is_darkstore = true THEN "Yes"
  ELSE "No"
  END AS isDmart 
FROM `peya-bi-tools-pro.il_core.fact_orders` o
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON p.partner_id = o.restaurant.id
WHERE o.registered_date BETWEEN from_date -1 AND to_date+1 AND DATE(o.registered_at_utc) BETWEEN from_date AND to_date
)o ON CAST(o.order_id AS STRING) = c.order_id
WHERE c.created_date BETWEEN from_date AND to_date AND c.stakeholder = 'Customer'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

contacts AS (
SELECT 
c.created_date,
country_name, 
vertical, 
delivery_type,
isDmart,
CASE WHEN c.order_id IS NOT NULL THEN "Order related" else 'Non order related' END AS order_related,  
CASE WHEN CCR1 = 'Live Order Process' THEN 'Live Contact' ELSE 'Non Live Contact' END AS live_contact,
CCR1,
CCR2,
CCR3,
NULL AS orders,
NULL AS sessions,
COUNT(DISTINCT contact_id) as contacts,
NULL AS csat_positive_response_all,
NULL AS csat_response_all,
NULL AS csat_response_ac,
NULL AS csat_response_ssf,
NULL AS csat_positive_response_ac,
NULL AS csat_positive_response_ssf,
NULL AS csat_triggered_ssf,
SUM(frc_base) AS Fcr_contacts,
SUM(is_fcr) AS Fcr,
SUM(recontact) as Recontact
FROM `peya-delivery-and-support.automated_tables_reports.cus_ops_contacts_perf` c
WHERE c.created_date BETWEEN from_date AND to_date
GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT * FROM 
(SELECT * FROM sessions
UNION ALL
SELECT * FROM orders
UNION ALL
SELECT * FROM csat
UNION ALL
SELECT * FROM contacts
 )