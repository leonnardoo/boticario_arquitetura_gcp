# Grupo Boticario - Arquitetura GCP
<img src="https://img.shields.io/badge/Challenge-Boticario-green"/> <img src="https://img.shields.io/badge/DataEngineer-ETL-brightgreen"/>

Repositório do case técnico do Grupo Boticário.

---

# Etapas a serem desenvolvidas:

- Pipeline de arquivos em excel
    - Carregar os dados da camada RAW para a camada TRUSTED
    - Carregar os arquivos para uma tabela no BigQuery
    - Modelar quatro novas tabelas com base na primeira

- Pipeline de dados vindos da API do Spotify
    - Carregar os dados da busca pelo termo "data hackers"
    - Carregar uma tabela no BigQuery com o resultado
    - Carregar os dados de todos os episodios do podcast DataHackers
    - Carregar uma tabela no BigQuery com o resultado
    - Modelar uma tabela somente com os episódios em que o Grupo Boticário é citado no podcast DataHackers

---
# Serviços da Google Cloud Platform utilizados:
- Cloud Storage
    - Serviço responsável pelo armazenamento dos arquivos em excel e dados da API. (raw e trusted)
- BigQuery
    - Serviço de Data Warehouse onde as tabelas modeladas foram disponibilizadas. (refined)
- Cloud Composer - Apache Airflow
    - Serviço de orquestração do fluxo de dados de todas as etapas 
- Cloud Build
    - Serviço responsável por sincronizar o projeto do GitHub com o Storage do Cloud Composer, garantindo o CI/CD do projeto.

- Outros softwares utilizados:
    - Visual Studio Code - ambiente de desenvolvimento (IDE) responsável por todos os testes de códigos

# Configurando os serviços

## Cloud Storage
- Criar os buckets:
    - raw_data_boticario
    - trusted_data_boticario

## BigQuery
- Criar os datasets para as tabelas modeladas:
    - refined
    - refined_api

## Cloud Composer
- Após subir o serviço do Composer instalar os pacotes pypi:
    - pandas==1.5.1
    - openpyxl==3.1.2
    - ndjson==0.3.1

- Criar as variáveis de ambiente
    *** Informações vindas do website https://developer.spotify.com/:
    - SPOTIFY_CLIENT_ID_API = client_id
    - SPOTIFY_CLIENT_SECRET_API = client_secret

## Cloud Build
- Criar um gatilho conectado ao repositório do GitHub deste projeto:
    - Evento: enviar para uma ramificação
    - Ramificação: main
    - Configuração: arquivo de configuração do Cloud Build (yaml ou json)
    - Local do arquivo de configuração: cloudbuild.yaml
    - Variáveis de substituição:
        - _GCS_BUCKET : endereço do bucket criado pelo Composer
    - E-mail da conta de serviço: Conta do Composer

---

# Trigando as DAGs manualmente

Depois de tudo configurado.

Pipeline de arquivos em Excel, executar na ordem:

- gb_ingest_data
    - gb_insert_data_vendas_ano_mes
    - gb_insert_data_vendas_linha_ano_mes
    - gb_insert_data_vendas_marca_ano_mes
    - gb_insert_data_vendas_marca_linha

Pipeline de dados da API, executar na ordem:

- gb_insert_api_data
    - gb_insert_api_data_podcast
    - gb_insert_api_data_podcast_episodes
        - gb_insert_api_data_spotify_podcast_episodes_gb

# Resultado

Ao final temos o seguinte cenário:
- GitHub
    - Todos os códigos versionados e com esteira de deploy
- Cloud Composer
    - Todas as DAGs orquestradas por hora UTC-3 (São Paulo)
- BigQuery
    - Tabelas modeladas prontas para uso do negócio
- Cloud Storage
    - Camadas raw e trusted (Data Lake)

Projeto finalizado.

# Licence
Licence MIT

# Author
Leonnardo Pereira - Data Engineer