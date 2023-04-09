SELECT
  EXTRACT(YEAR FROM DATA_VENDA) AS ANO,
  EXTRACT(MONTH FROM DATA_VENDA) AS MES,
  SUM(QTD_VENDA) AS SOMA_VENDA,
  {{ ts }} AS datahora_carga
FROM
  `singular-arcana-383119.refined.base_vendas_anos`
GROUP BY 1,2