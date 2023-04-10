SELECT
  MARCA,
  LINHA,
  SUM(QTD_VENDA) AS SOMA_VENDA,
  CURRENT_DATETIME('America/Sao_Paulo') AS datahora_carga
FROM
  `singular-arcana-383119.refined.base_venda_ano`
GROUP BY 1,2