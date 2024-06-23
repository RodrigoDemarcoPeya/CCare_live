DECLARE from_date DATE DEFAULT '2024-05-21'; DECLARE to_date DATE DEFAULT '2024-05-23';

CREATE OR REPLACE TABLE `peya-delivery-and-support.user_rodrigo_demarco.navigations_customer_hc`  
PARTITION BY created_date
CLUSTER BY global_entity_id, session_id

AS


SELECT
ns.created_date,
ns.navigation_id,
ns.session_id,
ns.global_entity_id,

--Page
ns.page_id,
ns.contact_reason_level_3,
ccr.contact_reason_l1,
ccr.contact_reason_l2,
ccr.contact_reason_l3,
ns.conditions, 
ns.resolution,
ns.leaf_feedback, 

--Navigation
ns.navigation_index, 
ns.num_chats_created,
ns.num_cases_created,


--Automations
ns.has_automation,
ns.automation_feature,
ns.automation_resolution,
ns.flow_version,

--Experience
ns.leaf_rating


FROM `peya-data-origins-pro.cl_gcc_service.hc_navigation_steps` ns

LEFT JOIN `peya-data-origins-pro.cl_gcc_service.global_contact_reasons` ccr ON ccr.contact_reason_l3 = ns.contact_reason_level_3

WHERE created_date BETWEEN from_date AND to_date
AND helpcenter = 'Customer'
AND ns.contact_reason_level_3 LIKE '1%%'