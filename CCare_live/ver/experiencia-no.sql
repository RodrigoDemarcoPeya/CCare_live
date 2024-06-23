DECLARE from_date DATE DEFAULT '2024-05-21'; DECLARE to_date DATE DEFAULT '2024-05-26';

CREATE OR REPLACE TABLE `peya-delivery-and-support.user_rodrigo_demarco.customer_care_hc_experience`
PARTITION BY created_date

AS


SELECT
s.created_date,
s.global_entity_id,
s.order_id,
s.session_id,

--CSAT
c.session_id as session_csat,
c.response_id,
c.survey_id,
c.contact_reason_l1 AS ccr1_csat,
c.contact_reason_l2 AS ccr2_csat,
c.contact_reason_l3 AS ccr3_csat,
c.q8,
c.csat,
c.qneutral,
CASE
  WHEN tr.session_id IS NOT NULL THEN 1
  ELSE 0
END AS trigger,
c.csat_resp,
c.csat_prom,
--Sessiones
s.session_segmentation_l1,
s.session_segmentation_l2,
s.session_experience_l1,
s.session_experience_l2,
s.session_type,
s.last_leaf_visited AS last_page_visited,
s.last_leaf_ccr,

FROM  `peya-data-origins-pro.cl_gcc_service.hc_sessions` s


LEFT JOIN (
  SELECT DISTINCT
  session_id
  FROM `peya-data-origins-pro.cl_gcc_service.hc_events` h
  WHERE created_date BETWEEN from_date and to_date
  AND event_name = 'SELF_SERVICE_CSAT_TRIGGERED'
) tr ON s.session_id = tr.session_id 

LEFT JOIN  (SELECT created_date, global_entity_id, session_id, response_id, chat_id, csat, ccr.global_cr_code, q8, qneutral, survey_id, contact_reason_l1, contact_reason_l2, contact_reason_l3,
  CASE WHEN csat IS NULL THEN NULL
  WHEN CSAT < 6 THEN 1
  ELSE NULL
  END AS csat_resp,
  CASE WHEN csat IS NULL THEN NULL
  WHEN CSAT > 3 THEN 1
  ELSE NULL
  END AS csat_prom,
FROM `peya-data-origins-pro.cl_gcc_service.tweety_csat_responses`
LEFT JOIN `peya-delivery-and-support.automated_tables_reports.global_contact_reasons` ccr ON ccr.global_cr_code = global_ccr_code        
WHERE stakeholder = 'Customer'
AND survey_type = 'Self Service' 
AND created_date BETWEEN from_date AND to_date
) c   ON  s.session_id = c.session_id 


WHERE s.created_date BETWEEN from_date AND to_date
AND s.helpcenter = 'Customer'
AND c.session_id IS NOT NULL
