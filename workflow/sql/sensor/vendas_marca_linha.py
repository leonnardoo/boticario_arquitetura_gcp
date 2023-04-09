VENDAS_MARCA_LINHA = f"""
         SELECT COUNT(*) = 1 AS should_continue
                 FROM `singular-arcana-383119.refined.__TABLES__` 
                     WHERE row_count > 0 
                       AND CAST(TIMESTAMP_MILLIS(last_modified_time) AS DATE) = CURRENT_DATE('America/Sao_Paulo')
                       AND table_id  = 'base_vendas_anos'
"""

SENSOR_QUERIES = {
    "vendas_marca_linha" : VENDAS_MARCA_LINHA
}