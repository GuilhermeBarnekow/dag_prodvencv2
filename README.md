Mongo → Snowflake • Pipeline ETL Incremental com Airflow

Resumo rápidoExemplo pronto para produção que extrai apenas os documentos novos/atualizados de uma coleção MongoDB, gera arquivos Parquet e carrega tudo em uma stage do Snowflake.Tabela de controle garante idempotência e um task final suspende o warehouse para economizar créditos.Todas as credenciais e nomes de objetos ficam fora do código‑fonte (Connections e Variables do Airflow).

⚙️ Visão Geral

MongoDB ──▶ Airflow (DAG) ──▶ Stage Snowflake ──▶ Tabela de Controle
   ▲                                   │
   └── filtro watermark (∆) ◀──────────┘

Incremental: consulta apenas documentos com dataalteracao > último upload.

Flatten: normaliza sub‑documentos metadata e funcionario.

Parquet + PUT: performance e compressão nativas do Snowflake.

Controle: grava nome do arquivo, data de upload, quantidade de linhas e flag de processado.

Custo: suspende o warehouse Snowflake ao final da carga.

🗂 Estrutura do Repositório

.
├── dags/
│   └── dag_mongo_to_snowflake.py   # Definição do DAG
├── tasks/
│   ├── tasks.py                    # Funções de ETL chamadas pelo DAG
│   └── utils.py                    # Helpers (credenciais, parsing)
├── requirements.txt
└── README.md                       # (este arquivo)

Ajuste o caminho do diretório dags/ de acordo com a variável de ambienteAIRFLOW__CORE__DAGS_FOLDER do seu deploy do Airflow.

🛠 Tecnologias Principais

Camada

Ferramenta/Lib

Função

Orquestração

Apache Airflow 2.8+

Agendamento, retries, logs

Fonte

MongoDB + pymongo

Consulta incremental

Transformação

pandas + pytz

Flatten + tratamento de time zone

Destino

Snowflake (snowflake‑connector-python)

Stage + tabela de controle

Configuração

Airflow Connections/Variables

Segredos e parâmetros externos

📋 Pré‑requisitos

Python 3.9+

Airflow 2.8+ com o provider snowflake instalado

Acesso de leitura a um cluster MongoDB

Usuário/role Snowflake com permissão de PUT, INSERT, UPDATE e ALTER WAREHOUSE

🚀 Como Usar

1. Clonar o projeto

git clone https://github.com/<seu‑org>/mongo-to-snowflake.git
cd mongo-to-snowflake

2. Instalar dependências

pip install -r requirements.txt

3. Criar conexões e variáveis no Airflow

Tipo

Chave/Nome

Exemplo / Observação

Connection

snowflake_default

account, user, warehouse, database, schema…

Variable

mongo_credentials

{"mongo_uri":"mongodb://<user>:<pwd>@host:27017","mongo_db":"mydb","mongo_collection":"minha_colecao"}

Variable

snowflake_schema_stage

@STAGE.DATALAKE

Variable

snowflake_schema_control

BRONZE

Variable

snowflake_timezone

America/Sao_Paulo (padrão)

Importante: não suba credenciais reais para o Git.

4. Ativar a DAG

Copie dags/dag_mongo_to_snowflake.py para o diretório de DAGs do Airflow.

No UI, habilite o toggle da DAG.

Dispare manualmente ou aguarde o agendamento (@daily por padrão).

🔍 Visão de Código

DAG (simplificado)

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
    arquivo   = extract_and_ingest()
    atualizar = update_processed_flags(arquivo)
    suspender = stop_warehouse()

    arquivo >> atualizar >> suspender

extract_and_ingest() – extrai, normaliza, grava Parquet e faz PUT.

update_processed_flags() – marca o arquivo como processado.

stop_warehouse() – suspende o warehouse (opcional).

🧩 Personalização

Precisa mudar

Onde alterar

Campo de watermark

tasks.py → get_mongo_documents()

Campos aninhados adicionais

tasks.py → process_documents()

Formato de arquivo

tasks.py → save_dataframe_to_parquet()

Frequência da DAG

dag_mongo_to_snowflake.py → schedule_interval

Suspender WH

Remover/editar stop_warehouse() se a política for diferente

❗ Problemas Comuns

Sintoma

Causa provável

Correção

“Nenhum documento novo”

Coluna/Timezone incorretos

Verifique filtro e fuso horário

MemoryError ao ler Mongo

DF grande demais

Paginar com batch_size() ou usar Dask

Warehouse não suspende

Uso concorrente

Ajuste stop_warehouse() ou multi‑cluster

PUT lento

Largura de banda da rede

Stage externa (S3/GCS) ou compressão maior

📄 Licença

Distribuído sob a MIT License.
