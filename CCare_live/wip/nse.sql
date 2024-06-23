DECLARE from_date DATE DEFAULT '2024-06-01';

DECLARE to_date DATE DEFAULT CURRENT_DATE();

SELECT  
	
non_seamless_order,
non_seamless_type	,
platform_order_code,
slow_order,
is_slow_order,
late_order,
is_late_order,
session_order,
is_session_order,
rejected_order,
is_rejected_order,
actual_delivery_time,
max_pdt,
late_order_bucket,
session_type,
live_order_session,
pdi_session
FROM `peya-datamarts-pro.dm_fulfillment.non_seamless_delivery_order_level` WHERE created_date_local BETWEEN from_date AND to_date