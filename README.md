# Mongo → Snowflake • Pipeline ETL Incremental com Airflow

## Introduction
Este projeto é um exemplo pronto para produção de um pipeline ETL incremental entre MongoDB e Snowflake utilizando Apache Airflow. Ele extrai apenas os documentos novos ou atualizados de uma coleção MongoDB, converte os dados em arquivos Parquet e os carrega em uma stage do Snowflake. Inclui controle de idempotência e otimização de custos ao suspender o warehouse automaticamente após o carregamento.

## Table of Contents
- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Technologies](#technologies)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Code Overview](#code-overview)
- [Customization](#customization)
- [Common Issues](#common-issues)
- [License](#license)

## Overview
```
MongoDB ──▶ Airflow (DAG) ──▶ Stage Snowflake ──▶ Tabela de Controle
    ▲                                        │
    └──────── filtro watermark (∆) ◀─────────┘
```

- **Incremental**: consulta apenas documentos com `dataalteracao > último upload`.
- **Flatten**: normaliza subdocumentos `metadata` e `funcionario`.
- **Parquet + PUT**: melhor performance e compressão nativas do Snowflake.
- **Controle**: grava nome do arquivo, data de upload, quantidade de linhas e flag de processado.
- **Custo**: suspende o warehouse após a carga.

## Repository Structure
```
.
├── dags/
│   └── dag_mongo_to_snowflake.py      # Definição do DAG
├── tasks/
│   ├── tasks.py                       # Funções de ETL
│   └── utils.py                       # Helpers para credenciais e parsing
├── requirements.txt
└── README.md                          # Este arquivo
```

## Technologies

| Camada        | Ferramenta/Lib                    | Função                                     |
|---------------|-----------------------------------|--------------------------------------------|
| Orquestração  | Apache Airflow 2.8+               | Agendamento, retries, logs                 |
| Fonte         | MongoDB + pymongo                 | Consulta incremental                       |
| Transformação | pandas + pytz                     | Flatten + tratamento de time zone         |
| Destino       | Snowflake + snowflake-connector   | Stage + tabela de controle                 |
| Configuração  | Airflow Connections/Variables     | Gerenciamento de segredos e parâmetros     |

## Prerequisites
- Python 3.9+
- Apache Airflow 2.8+ com o provider do Snowflake instalado
- Acesso de leitura ao MongoDB
- Permissões Snowflake: PUT, INSERT, UPDATE e ALTER WAREHOUSE

## Installation

```bash
git clone https://github.com/<seu-org>/mongo-to-snowflake.git
cd mongo-to-snowflake
pip install -r requirements.txt
```

## Usage

1. **Criar conexões e variáveis no Airflow**:

| Tipo       | Nome/Chave               | Exemplo / Observação                                       |
|------------|---------------------------|------------------------------------------------------------|
| Connection | `snowflake_default`       | Informar account, user, warehouse, database, schema etc.   |
| Variable   | `mongo_credentials`       | `{"mongo_uri":"...","mongo_db":"...","mongo_collection":"..."}` |
| Variable   | `snowflake_schema_stage`  | `@STAGE.DATALAKE`                                          |
| Variable   | `snowflake_schema_control`| `BRONZE`                                                   |
| Variable   | `snowflake_timezone`      | `America/Sao_Paulo` (padrão)                               |

> **Atenção**: nunca suba credenciais reais para o Git.

2. **Ativar a DAG**:
   - Copie o `dag_mongo_to_snowflake.py` para o diretório de DAGs.
   - Ative a DAG na interface do Airflow.
   - Dispare manualmente ou aguarde o agendamento (`@daily` por padrão).

## Code Overview

### DAG (resumo simplificado)
```python
with DAG(
    dag_id="mongo_to_sf_incremental",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 5,
        "retry_delay": timedelta(minutes=2)
    },
    tags=["etl", "mongo", "snowflake"]
) as dag:
    arquivo = extract_and_ingest()
    atualizar = update_processed_flags(arquivo)
    suspender = stop_warehouse()

    arquivo >> atualizar >> suspender
```

- `extract_and_ingest()` – Extrai, normaliza, salva Parquet e envia com PUT.
- `update_processed_flags()` – Marca os arquivos como processados.
- `stop_warehouse()` – Suspende o warehouse (opcional).

## Customization

| O que mudar                    | Onde alterar                             |
|-------------------------------|------------------------------------------|
| Campo de watermark            | `tasks.py → get_mongo_documents()`       |
| Campos aninhados extras       | `tasks.py → process_documents()`         |
| Formato de arquivo            | `tasks.py → save_dataframe_to_parquet()` |
| Frequência da DAG             | `dag_mongo_to_snowflake.py → schedule_interval` |
| Política de suspensão de WH   | Modificar/remover `stop_warehouse()`     |

## Common Issues

| Sintoma                      | Causa provável         | Solução                                        |
|-----------------------------|------------------------|------------------------------------------------|
| Nenhum documento novo       | Timezone/filtro errado | Verifique campo e fuso horário                 |
| MemoryError ao ler MongoDB  | DataFrame muito grande | Paginar via `batch_size()` ou usar Dask        |
| Warehouse não suspende      | Uso concorrente        | Ajustar `stop_warehouse()` ou usar multi-cluster |
| PUT lento                   | Rede                    | Usar stage externa (S3/GCS) ou compressão maior |

## License
Distribuído sob a [MIT License](LICENSE).
