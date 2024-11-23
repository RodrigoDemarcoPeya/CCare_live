
DECLARE from_date DATE DEFAULT '2024-07-01'; DECLARE to_date DATE DEFAULT '2024-11-21';

create or replace table `peya-delivery-and-support.automated_tables_reports.cus_ops_contacts_perf` 
partition by created_date as 

WITH
contacts AS (
SELECT
c.contact_id,
created_date,
RIGHT(global_entity_id,2) AS country_name, 
creation_timestamp AS created_at_utc,
resolution_timestamp AS resolution_at_utc,
c.case_origin,
CASE WHEN c.contact_reason_l1 = 'Contact without CR' THEN 'Without CCR1'ELSE c.contact_reason_l1 END AS CCR1,
CASE WHEN c.contact_reason_l2 = 'Contact without CR' THEN 'Without CCR2'ELSE c.contact_reason_l2 END AS CCR2,
CASE WHEN c.contact_reason_l3 = 'Contact without CR' THEN 'Without CCR3'ELSE c.contact_reason_l3 END AS CCR3,
c.local_contact_reason AS CCR4,
handling_time_secs AS AHT,
first_reply_time_secs AS FRT,
s.queue_time,
is_sla30_fa_ind,
avg_r_time,
is_fcr_ind,
is_transf_chat_ind,
ga_ind,
sat_comment,
payment_method
lob,
o.delivery_type,
c.is_preorder,
o.vertical,
c.order_id,
stakeholder_entry_point,
CASE
    WHEN datetime_diff(
        DATETIME(f.rejected_at),
        DATETIME(creation_timestamp),
        SECOND
    ) >= 0
    AND datetime_diff(
        f.rejected_at,
        DATETIME(resolution_timestamp),
        SECOND
    ) <= 0
    AND accionador_level2 = 'CUSTOMER SERVICE' THEN TRUE
    ELSE false
  END AS rejected_in_this_chat,

FROM `fulfillment-dwh-production.curated_data_shared.all_contacts` c
LEFT JOIN `peya-delivery-and-support.automated_tables_reports.ops_contacts` s ON s.contact_id = c.contact_id AND s.service = 'Customer' AND created_date_Mvd BETWEEN from_date-1 AND to_date+1 
LEFT JOIN `peya-datamarts-pro.dm_fulfillment.fail_rate_order_level` f ON SAFE_CAST(f.order_id AS INT) = s.order_id AND f.registered_date BETWEEN from_date AND to_date
LEFT JOIN (SELECT
  order_id,
  business_type.business_type_name AS vertical,
  delivery_type,
  order_status
  FROM `peya-bi-tools-pro.il_core.fact_orders` 
  WHERE registered_date BETWEEN from_date-1 AND to_date+1 
  )o ON CAST(o.order_id AS STRING) = c.order_id

WHERE c.stakeholder = 'Customer' AND c.created_date between from_date and to_date AND contact_resolution_skill != 'courier-business' 
)

SELECT
s.*,

FROM contacts s
WHERE s.created_date BETWEEN from_date AND to_date


