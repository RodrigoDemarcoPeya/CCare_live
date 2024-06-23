DECLARE from_date DATE DEFAULT '2024-05-01'; DECLARE to_date DATE DEFAULT '2024-05-27';

CREATE OR REPLACE TABLE `peya-delivery-and-support.user_rodrigo_demarco.customer_contacts`  
PARTITION BY created_date

AS

SELECT
p.created_date,
p.chat_id,
p.global_entity_id,
p.stakeholder,
p.order_id,
p.contact_reason_l1,
p.contact_reason_l2,
p.contact_reason_l3,
p.hc_identifier,

--CHAT
p.resolution_status,
p.first_reply_time_secs,
p.first_reply_time_live_agent_secs,
p.first_agent_message_count,

--CSAT
csat,
csat_resp,
csat_prom,
c.contact_reason_l1 AS ccr1_csat, 
c.contact_reason_l2 AS ccr2_csat,
c.contact_reason_l3 AS ccr3_csat,

--FRC
channel,
next_contacts,
is_fcr,

--HC
hc.hc_ccr2,
hc.hc_ccr3,
hc.order_status as hc_order_status,
hc.page_id,


--TBD
CASE
WHEN comp.order_id IS NOT NULL THEN 1
ELSE 0
END AS compensated_contact,

--FR Origin
fr.rejected_at,
fr.accionador_level2,
CASE
  WHEN datetime_diff(fr.rejected_at, datetime(p.created_date),SECOND) >= 0
  AND datetime_diff(fr.rejected_at, datetime(p.resolution_timestamp),SECOND) <= 0 THEN TRUE
  ELSE false
END AS rejected_in_chat,

CASE
  WHEN datetime_diff(fr.rejected_at,datetime(p.created_date),SECOND) >= 0
  AND datetime_diff(fr.rejected_at,datetime(p.resolution_timestamp),SECOND) <= 0
  AND accionador_level2 = 'CUSTOMER_SERVICE' THEN TRUE ELSE false 
END AS rejected_in_this_chat,

--Payment
ord.payment_type

FROM `peya-data-origins-pro.cl_gcc_service.pandacare_chats` p

LEFT JOIN  (SELECT created_date, global_entity_id, response_id, chat_id, csat, ccr.global_cr_code, qcomment, qneutral, contact_reason_l1, contact_reason_l2, contact_reason_l3,
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
AND survey_type = 'After Contact' 
AND created_date BETWEEN from_date AND to_date
) c   ON  c.chat_id = p.hc_identifier

LEFT JOIN(SELECT
ticket_id,
order_id,
next_contacts,
channel,
is_fcr,
FROM `peya-datamarts-pro.dm_care.first_contact_resolution`
WHERE DATE(partition_date) BETWEEN from_date AND to_date
) f ON f.ticket_id = p.chat_id

LEFT JOIN (
SELECT
DISTINCT h.contact_id,
od.status AS order_status,
page_id,
ccr.contact_reason_l2 AS hc_ccr2,
ccr.contact_reason_l3 AS hc_ccr3
    FROM
        `peya-data-origins-pro.cl_gcc_service.hc_events` h,
        unnest(order_details) od
LEFT JOIN `peya-delivery-and-support.automated_tables_reports.global_contact_reasons` ccr ON ccr.global_cr_code = h.contact_reason_level_3  
WHERE created_date BETWEEN from_date AND to_date
AND contact_id IS NOT NULL
AND event_name = 'CHAT_ESTABLISHED'
AND page_id <> 'orderDetail') hc ON hc.contact_id = p.hc_identifier

LEFT JOIN (
SELECT
DISTINCT SAFE_CAST(ov.order_id AS INT64) AS order_id,
ov.agent_email,
ov.created_at,
ov.contact_id,
FROM
`peya-data-origins-pro.cl_gcc_service.oneview_events_v2` ov
LEFT JOIN `peya-data-origins-pro.cl_compensations.user_compensations` C ON safe_cast(ov.order_id AS int64) = c.order_id
AND lower(ov.agent_email) = lower(c.agent.email)
AND c.reason IN (
'Entrega fuera de los plazos estimado',
'No entregado por el cadete',
'Pedido no entregado',
'Orden demorada'
)
AND c.type = 'AGENT_COMPENSATION'
WHERE created_date BETWEEN from_date AND to_date
AND ov.event_name = 'ActionsCompensationDone'
AND ov.action_outcome = 'success'
AND c.order_id IS NOT NULL
) AS comp ON comp.order_id = safe_cast(p.order_id AS int64)
AND comp.contact_id = p.chat_id 

LEFT JOIN `peya-datamarts-pro.dm_fulfillment.fail_rate_order_level` fr ON safe_cast(fr.order_id AS int64) = safe_cast(p.order_id AS int64)

LEFT JOIN(
  SELECT
registered_date, order_id, paymentMethod.name as payment_type
  FROM `peya-bi-tools-pro.il_core.fact_orders`
  WHERE registered_date BETWEEN from_date AND to_date
) ord ON CAST(ord.order_id AS STRING) = p.order_id



WHERE p.created_date BETWEEN from_date AND to_date AND p.stakeholder = 'Customer'