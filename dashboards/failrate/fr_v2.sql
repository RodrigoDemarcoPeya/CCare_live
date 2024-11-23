DECLARE from_date DATE DEFAULT '2024-07-01'; DECLARE to_date DATE DEFAULT '2024-10-27';


CREATE OR REPLACE TABLE `peya-delivery-and-support.automated_tables_reports.cus_ops_fail_rate_v2`
PARTITION BY order_date
CLUSTER BY country_name, status

AS

WITH orders AS (
    SELECT DISTINCT 
        DATE(o.registered_at) AS order_date,
        o.country.country_code AS country_name,
        COUNT(DISTINCT o.order_id) OVER (PARTITION BY DATE(o.registered_at)) AS o_day,
        COUNT(DISTINCT o.order_id) OVER (PARTITION BY DATE_TRUNC(DATE(o.registered_at),ISOWEEK)) AS o_week,
        COUNT(DISTINCT o.order_id) OVER (PARTITION BY DATE_TRUNC(DATE(o.registered_at),MONTH)) AS o_month,
        COUNT(DISTINCT o.order_id) OVER (PARTITION BY DATE(o.registered_at), o.country.country_name_en) AS o_day_c,
        COUNT(DISTINCT o.order_id) OVER (PARTITION BY DATE_TRUNC(DATE(o.registered_at), ISOWEEK), o.country.country_name_en) AS o_week_c,
        COUNT(DISTINCT o.order_id) OVER (PARTITION BY DATE_TRUNC(DATE(o.registered_at), MONTH), o.country.country_name_en) AS o_month_c,

    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o 

    WHERE 
        o.registered_date BETWEEN from_date-1 AND to_date+1
    ORDER BY 1 DESC
), 

failed_orders AS (
  SELECT DISTINCT
    DATE(o.registered_at) AS order_date,
    o.country.country_code AS country_name,
    o.order_status AS status,
    UPPER(o.business_type.business_type_name) AS vertical_type,
    o.is_online_payment AS online_payment,
    CASE
      WHEN o.business_type_id IN (9, 11) THEN 'COURIER'
      WHEN is_take_out = 1 THEN 'PICK_UP'
      WHEN is_take_out = 0 AND o.with_logistics = TRUE THEN 'OWN_DELIVERY'
      WHEN is_take_out = 0 AND o.with_logistics = FALSE THEN 'VENDOR_DELIVERY'
    END AS delivery_type,
    fr.reject_reason,
    fr.fr_owner_peya as fr_owner,
    fr.accionador_level1,
    fr.accionador_level2,
    fr.cancel_tool,
    fr.autocancellation,
    fr.vendor_accepted_before_reject,
    fr.cancel_moment_customer_facing,
    fr.rejected_pdt_state,
    fr.minutes_to_cancel,
  CASE
    WHEN fr.rider_status IS NULL THEN NULL
    WHEN fr.rejected_at IS NULL THEN NULL
    WHEN fr.rider_status = 'pending' THEN concat('a. ',rider_status)
    WHEN fr.rider_status = 'scheduled' THEN concat('b. ',rider_status)
    WHEN fr.rider_status = 'queued' THEN concat('c. ',rider_status)
    WHEN fr.rider_status = 'dispatched' THEN concat('d. ',rider_status)
    WHEN fr.rider_status = 'courier_notified' THEN concat('e. ',rider_status)
    WHEN fr.rider_status = 'accepted' THEN concat('f. ',rider_status)
    WHEN fr.rider_status = 'near_pickup' THEN concat('g. ',rider_status)
    WHEN fr.rider_status = 'picked_up' THEN concat('h. ',rider_status)
    WHEN fr.rider_status = 'left_pickup' THEN concat('i. ',rider_status)
    WHEN fr.rider_status = 'near_dropoff' THEN concat('j. ',rider_status)
    WHEN fr.rider_status = 'completed' THEN concat('k. ',rider_status)
    WHEN fr.rider_status = 'cancelled' THEN concat('l. ',rider_status)
    ELSE NULL
  END AS rider_status_at_reject,

    CASE
        WHEN o.order_status != 'REJECTED' THEN NULL
        WHEN cancel_moment_customer_facing = '6-After dropoff' THEN '9. delivered'
        WHEN minutes_to_cancel < 5 THEN '1. 00 to 05 min'
        WHEN minutes_to_cancel < 10 THEN '2. 05 to 10 min'
        WHEN minutes_to_cancel < 20 THEN '3. 10 to 20 min'
        WHEN minutes_to_cancel < 30 THEN '4. 20 to 30 min'
        WHEN minutes_to_cancel < 40 THEN '5. 30 to 40 min'
        WHEN minutes_to_cancel < 50 THEN '6. 40 to 50 min'
        WHEN minutes_to_cancel < 60 THEN '7. 50 to 60 min'
        WHEN minutes_to_cancel >= 60 THEN '8. over 60 min'
        ELSE NULL
    END AS created_to_reject_bk,

    cn.customer_issue,
    CASE
      WHEN cn.customer_issue IS NOT NULL THEN TRUE 
      ELSE FALSE
    END AS has_customer_issue,

    CASE
      WHEN customer_request_cancel IS NULL THEN NULL
      WHEN customer_request_cancel = false THEN NULL
      WHEN accionador_level2 = 'USER SELF MANAGED'
      AND vendor_accepted_before_reject = false THEN 'self_cancel_bf_vendor_acceptance'
      WHEN accionador_level2 = 'USER SELF MANAGED'
      AND vendor_accepted_before_reject = TRUE
      AND cancel_moment_customer_facing = '6-After dropoff' THEN 'self_cancel_delivered'
      WHEN accionador_level2 = 'USER SELF MANAGED'
      AND vendor_accepted_before_reject = TRUE
      AND cancel_moment_customer_facing != '6-After dropoff'
      AND rejected_pdt_state NOT IN ('4.Late more than 10', '3.Late') THEN 'self_cancel_af_vendor_acceptance'
      WHEN accionador_level2 = 'USER SELF MANAGED'
      AND vendor_accepted_before_reject = TRUE
      AND cancel_moment_customer_facing != '6-After dropoff'
      AND rejected_pdt_state IN ('4.Late more than 10', '3.Late') THEN 'self_cancel_delay'
      WHEN accionador_level2 != 'USER SELF MANAGED'
      AND vendor_accepted_before_reject = false THEN 'cs_cancel_bf_vendor_acceptance'
      WHEN accionador_level2 != 'USER SELF MANAGED'
      AND vendor_accepted_before_reject = TRUE
      AND cancel_moment_customer_facing = '6-After dropoff' THEN 'cs_cancel_delivered'
      WHEN accionador_level2 != 'USER SELF MANAGED'
      AND vendor_accepted_before_reject = TRUE
      AND cancel_moment_customer_facing != '6-After dropoff'
      AND rejected_pdt_state NOT IN ('4.Late more than 10', '3.Late') THEN 'cs_cancel_af_vendor_acceptance'
      WHEN accionador_level2 != 'USER SELF MANAGED'
      AND vendor_accepted_before_reject = TRUE
      AND cancel_moment_customer_facing != '6-After dropoff'
      AND rejected_pdt_state IN ('4.Late more than 10', '3.Late') THEN 'cs_cancel_delay'
      ELSE 'other'
    END AS customer_cancellation_type,

  COUNT(DISTINCT o.order_id) as total_orders,
  COUNT(CASE WHEN o.order_status = 'REJECTED' THEN o.order_id ELSE NULL END) AS fail_order,

  COUNT(CASE WHEN o.order_status = 'REJECTED' and fr.accionador_level1 = 'USER' THEN o.order_id ELSE NULL END) AS cus_fail_rate,
  COUNT(CASE WHEN o.order_status = 'REJECTED' and fr.accionador_level2 = 'USER SELF MANAGED' THEN o.order_id ELSE NULL END) AS cus_self_fail_rate,
  COUNT(CASE WHEN o.order_status = 'REJECTED' and fr.cancel_tool IN ('HC_DEEPLINK_AUTOCANCELLATION','HC_AUTOCANCELLATION') THEN o.order_id ELSE NULL END) AS hc_autocancel,
  COUNT(CASE WHEN o.order_status = 'REJECTED' and fr.accionador_level2 = 'CUSTOMER SERVICE' THEN o.order_id ELSE NULL END) AS agent_fail_rate,
  COUNT(CASE WHEN o.order_status = 'REJECTED' and fr.cancel_tool = 'OM_AUTOCANCELLATION' THEN o.order_id ELSE NULL END) AS otp_fail_rate,



  FROM
    `peya-bi-tools-pro.il_core.fact_orders` o

  LEFT JOIN 
    `peya-datamarts-pro.dm_fulfillment.fail_rate_order_level` fr  on safe_cast(fr.order_id as int64) = o.order_id AND fr.registered_date  BETWEEN from_date-1 AND to_date+1    

    LEFT JOIN
  (  SELECT DISTINCT
      SAFE_CAST(content.order_id as int64) order_id,
      content.customer_notification_type as customer_issue,
      ROW_NUMBER() OVER (PARTITION BY content.order_id order by timestamp) as rn
   
    FROM 
      `peya-data-origins-pro.cl_hurrier.delivery_notification_command` n 
    WHERE date(timestamp) BETWEEN from_date AND to_date
  ) cn on cn.order_id = o.order_id  AND rn = 1

  WHERE
    o.registered_date BETWEEN from_date-1 AND to_date+1
  GROUP BY ALL
)

select
f.*,
o.o_day,
o_week,
o.o_month,

    SAFE_DIVIDE(f.fail_order, o.o_day) fr_day,
    SAFE_DIVIDE(f.fail_order, o.o_week) fr_week,
    SAFE_DIVIDE(f.fail_order, o.o_month) fr_month,
    SAFE_DIVIDE(f.fail_order, o.o_day_c) fr_day_c,
    SAFE_DIVIDE(f.fail_order, o.o_week_c) fr_week_c,
    SAFE_DIVIDE(f.fail_order, o.o_month_c) fr_month_c,
from failed_orders f
LEFT JOIN orders AS o 
    ON o.order_date = f.order_date 
    AND o.country_name = f.country_name

WHERE o.order_date BETWEEN from_date AND to_date AND f.order_date BETWEEN from_date AND to_date

