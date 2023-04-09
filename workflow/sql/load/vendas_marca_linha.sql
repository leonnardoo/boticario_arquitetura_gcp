SELECT
  MARCA,
  LINHA,
  SUM(QTD_VENDA) AS SOMA_VENDA,
FROM
  `singular-arcana-383119.refined.base_vendas_anos`
GROUP BY 1,2