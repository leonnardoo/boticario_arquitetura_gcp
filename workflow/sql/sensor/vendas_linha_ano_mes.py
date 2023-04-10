VENDAS_LINHA_ANO_MES = f"""
         SELECT COUNT(*) = 1 AS should_continue
                 FROM `singular-arcana-383119.refined.__TABLES__` 
                     WHERE row_count > 0 
                       AND CAST(TIMESTAMP_MILLIS(last_modified_time) AS DATE) = CURRENT_DATE('America/Sao_Paulo')
                       AND table_id  = 'base_venda_ano'
"""

SENSOR_QUERIES = {
    "vendas_linha_ano_mes" : VENDAS_LINHA_ANO_MES
}