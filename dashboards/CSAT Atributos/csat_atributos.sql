DECLARE from_date DATE DEFAULT '2024-11-10'; DECLARE to_date DATE DEFAULT '2024-11-27';

/*
Owner: Rodrigo Demarco
Descripcion:
Query para el seguimiento de los atributos de la encuesta de CSAT, tanto para SSF como para AC.
*/

CREATE OR REPLACE TABLE `peya-delivery-and-support.automated_tables_reports.cusops_csat_atributos`  
PARTITION BY created_date
CLUSTER BY CCR1,CCR2,CCR3

AS

SELECT 
  created_date,
    session_id,    
    response_id,
    RIGHT(global_entity_id,2) AS country,
    survey_type,
    csat,
    contact_reason_l1 AS CCR1, 
    contact_reason_l2 AS CCR2,
    contact_reason_l3 AS CCR3,
    qneutral AS atributo,
    qcomment AS comment,
    SPLIT(qneutral, ',') AS atributo_individual,
    count(distinct case when csat < 6 AND survey_type = 'After Contact' THEN response_id ELSE NULL END) AS csat_response_ac,
    count(distinct case when csat < 6 AND survey_type = 'Self Service' THEN response_id ELSE NULL END) AS csat_response_ssf,
    COUNT(distinct case when csat > 3 AND survey_type = 'After Contact' THEN response_id ELSE NULL END) AS csat_positive_response_ac,
    COUNT(distinct case when csat > 3 AND survey_type = 'Self Service' THEN response_id ELSE NULL END) AS csat_positive_response_ssf,
FROM `peya-data-origins-pro.cl_gcc_service.tweety_csat_responses` c
LEFT JOIN `peya-delivery-and-support.automated_tables_reports.global_contact_reasons` AS GC ON global_ccr_code = global_cr_code AND contact_category = 'Customer'

WHERE 
    stakeholder = 'Customer' AND
    c.created_date BETWEEN from_date AND to_date

  GROUP BY all
