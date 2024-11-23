
DECLARE from_date DATE DEFAULT '2024-07-01'; DECLARE to_date DATE DEFAULT '2024-11-21';
CREATE OR REPLACE TABLE `peya-delivery-and-support.automated_tables_reports.cusOps_session_level`  
PARTITION BY created_date

AS
SELECT
*
FROM (
  SELECT 
  s.session_id,
  DATE(	s.created_at) AS created_date,
  RIGHT(s.global_entity_id, 2) AS country_name, 
  CASE WHEN gc.contact_reason_l1 IS NULL THEN 'Without CCR1' ELSE gc.contact_reason_l1 END as CCR1,
  CASE WHEN gc.contact_reason_l2 IS NULL THEN 'Without CCR2' ELSE gc.contact_reason_l2 END as CCR2,
  CASE WHEN gc.contact_reason_l3 IS NULL THEN 'Without CCR3' ELSE gc.contact_reason_l3 END as CCR3,
  s.device_type,
  s.first_order_status,
  s.last_order_status,
  s.time_between_order_and_session_sec,
  s.session_type,
  s.session_segmentation_l1,
  s.session_segmentation_l2,
  s.session_experience_l1, 
  s.session_experience_l2, 
  s.order_id,
  s.first_contact_id,
  csat_trigger,
  template,
  o.order_id as oorder,
  o.vertical,
  o.delivery_type,
  o.order_status
  FROM  `peya-data-origins-pro.cl_gcc_service.hc_sessions`s
  LEFT JOIN `peya-delivery-and-support.automated_tables_reports.global_contact_reasons` AS GC ON s.last_leaf_ccr = global_cr_code AND contact_category = 'Customer'
  LEFT JOIN `peya-bi-tools-pro.il_hcc.fact_helpcenter_sessions_level` se ON se.session_id = s.session_id AND se.created_date BETWEEN from_date and to_date
  LEFT JOIN (SELECT
  order_id,
  business_type.business_type_name AS vertical,
  delivery_type,
  order_status
  FROM `peya-bi-tools-pro.il_core.fact_orders` 
  WHERE registered_date BETWEEN from_date-1 AND to_date+1 
  )o ON CAST(o.order_id AS STRING) = s.order_id
  WHERE s.created_date BETWEEN from_date-1 AND to_date+1 AND s.stakeholder = 'Customer'
)WHERE created_date between from_date and to_date