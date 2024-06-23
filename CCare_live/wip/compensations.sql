SELECT DISTINCT
EXTRACT(ISOWEEK FROM original_order_date) AS Week,
ca.backoffice_reason,
COUNT(ca.id) as cuenta,

 FROM `peya-bi-tools-pro.il_compensations.fact_compensations_and_refunds_care`  c,
 UNNEST(cor) ca
 
 WHERE original_order_date > '2024-06-01'
 AND ca.source = 'HELP_CENTER'
 AND ca.backoffice_reason IN ('Orden demorada','Pedido no entregado')


 GROUP BY 1,2
 ORDER BY 1 ASC