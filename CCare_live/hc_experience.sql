DECLARE from_date DATE DEFAULT DATE_ADD(CURRENT_DATE, INTERVAL -10 DAY);

DECLARE to_date DATE DEFAULT CURRENT_DATE();

DELETE FROM `peya-delivery-and-support.user_rodrigo_demarco.customer_care_hc_experience` where created_date BETWEEN from_date AND to_date


DECLARE from_date DATE DEFAULT DATE_ADD(CURRENT_DATE, INTERVAL -10 DAY);

DECLARE to_date DATE DEFAULT CURRENT_DATE();


INSERT INTO `peya-delivery-and-support.user_rodrigo_demarco.customer_care_hc_experience` (created_date,global_entity_id, order_id, session_id, session_csat, response_id, survey_id, ccr1_csat, ccr2_csat, ccr3_csat, qcomment, csat, qneutral, trigger, csat_resp, csat_prom, session_segmentation_l1, session_segmentation_l2, session_experience_l1, session_experience_l2, session_type, last_page_visited, last_leaf_ccr, ccr1_session, ccr2_session, ccr3_session, Equipo, issue, non_seamless_order, non_seamless_type)



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
c.qcomment,
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
ccr.contact_reason_l1 AS ccr1_session,
ccr.contact_reason_l2 AS ccr2_session,
ccr.contact_reason_l3 AS ccr3_session,
CASE WHEN s.last_leaf_ccr LIKE '1%%' THEN 'Live' ELSE 'NonLive' END AS Equipo,

--Issues
i.issue,

--NSE
nse.non_seamless_order,
nse.non_seamless_type	,


FROM  `peya-data-origins-pro.cl_gcc_service.hc_sessions` s
LEFT JOIN `peya-delivery-and-support.automated_tables_reports.global_contact_reasons` ccr ON ccr.global_cr_code = s.last_leaf_ccr      

LEFT JOIN (
  SELECT DISTINCT
  session_id
  FROM `peya-data-origins-pro.cl_gcc_service.hc_events` h
  WHERE created_date BETWEEN from_date and to_date
  AND event_name = 'SELF_SERVICE_CSAT_TRIGGERED'
) tr ON s.session_id = tr.session_id 

LEFT JOIN (

SELECT
content.order_id,
content.customer_notification_type AS issue,
MAX(r.timestamp) AS cus_issue_at,
FROM `peya-data-origins-pro.cl_hurrier.delivery_notification_command` r
WHERE DATE(r.timestamp) BETWEEN from_date AND to_date
GROUP BY 1,2
 ) i ON i.order_id = s.order_id

LEFT JOIN  (SELECT created_date, global_entity_id, session_id, response_id, chat_id, csat, ccr.global_cr_code, qcomment, qneutral, survey_id, contact_reason_l1, contact_reason_l2, contact_reason_l3,
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


LEFT JOIN (SELECT
non_seamless_order,
non_seamless_type	,
platform_order_code,
FROM `peya-datamarts-pro.dm_fulfillment.non_seamless_delivery_order_level` 
WHERE created_date_local BETWEEN from_date AND to_date
 ) nse ON CAST(nse.platform_order_code as string) = s.order_id


WHERE s.created_date BETWEEN from_date AND to_date
AND s.helpcenter = 'Customer'
