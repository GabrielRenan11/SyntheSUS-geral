# SyntheSUS

O **SyntheSUS** é uma solução de engenharia e análise de dados voltada à saúde pública brasileira, desenvolvida para facilitar a interpretação de informações provenientes do **DATASUS**, com foco em internações hospitalares, leitos, equipamentos, profissionais e unidades de saúde.

O projeto integra coleta, transformação, armazenamento e disponibilização dos dados por meio de uma arquitetura em camadas **Bronze, Silver e Gold**, utilizando Apache Airflow e Docker para orquestração, Oracle Cloud Infrastructure para armazenamento, Oracle Database para persistência analítica, FastAPI para exposição dos dados e uma aplicação web para visualização dos indicadores.

---

## Arquitetura

```text
DATASUS
   │
   ├── CNES
   │   ├── Estabelecimentos
   │   ├── Leitos
   │   ├── Equipamentos
   │   └── Profissionais
   │
   └── SIH/SUS
       └── Internações
            │
            ▼
        ┌─────────┐
        │ Bronze  │
        │ Dados   │
        │ brutos  │
        └────┬────┘
             │
             ▼
        ┌─────────┐
        │ Silver  │
        │ Limpeza │
        │ e       │
        │ padron. │
        └────┬────┘
             │
             ▼
        ┌─────────┐
        │  Gold   │
        │ Dados   │
        │ analít. │
        └────┬────┘
             │
             ▼
      Oracle Database
             │
             ▼
          FastAPI
             │
             ▼
     SyntheSUS Dashboard
```

---

## Estrutura do repositório

```text
SyntheSUS/
│
├── camadas/
│   ├── airflow/
│   ├── ingestao_synthesus.py
│   ├── script_transformacao_silver.py
│   ├── script_transformacao_gold.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── banco/
│   ├── airflow/
│   ├── src/
│   ├── Dockerfile
│   └── requirements.txt
│
├── api/
│   ├── main.py
│   ├── database.py
│   ├── requirements-api.txt
│   └── .python-version
│
├── frontend/
│   ├── assets/
│   ├── index.html
│   ├── styles.css
│   └── app.js
│
├── .gitignore
└── README.md
```

---

## Pipeline de dados

### Bronze

A camada **Bronze** é responsável pela ingestão dos dados em seu formato mais próximo possível da fonte original.

Os dados são obtidos principalmente a partir dos sistemas públicos disponibilizados pelo DATASUS e armazenados no **Oracle Cloud Infrastructure Object Storage**.

Entre as fontes utilizadas estão:

- CNES — Cadastro Nacional de Estabelecimentos de Saúde;
- SIH/SUS — Sistema de Informações Hospitalares;
- dados de estabelecimentos;
- leitos hospitalares;
- equipamentos;
- profissionais;
- internações.

---

### Silver

A camada **Silver** realiza a limpeza, padronização e enriquecimento dos dados provenientes da Bronze.

Entre as principais transformações estão:

- padronização de tipos;
- tratamento de campos;
- criação de identificadores;
- organização temporal;
- enriquecimento de códigos de ocupação;
- enriquecimento de diagnósticos;
- padronização de informações de leitos e equipamentos;
- preparação dos dados para utilização analítica.

---

### Gold

A camada **Gold** transforma os dados tratados em estruturas voltadas ao consumo analítico e à carga no banco de dados.

Entre as principais entidades utilizadas estão:

- `DIM_DATA`;
- `DIM_MUNICIPIO`;
- `DIM_ESTABELECIMENTO`;
- `UNIDADE_SAUDE`;
- `INTERNACAO`;
- `FATO_LEITOS`;
- `FATO_EQUIPAMENTOS`;
- `PROFISSIONAL`;
- `ALVARA`.

---

## Orquestração

O pipeline utiliza **Apache Airflow** para controlar a execução das etapas de ingestão e transformação.

Os componentes são executados em containers utilizando **Docker**, permitindo maior isolamento e reprodutibilidade do ambiente.

Fluxo principal:

```text
Ingestão
   ↓
Bronze
   ↓
Transformação Silver
   ↓
Silver
   ↓
Transformação Gold
   ↓
Gold
   ↓
Carga no Oracle
```

---

## Banco de dados

Os dados tratados são carregados em um **Oracle Database**, utilizado como base para as consultas analíticas realizadas pela API.

A modelagem permite relacionar informações de:

- municípios;
- unidades de saúde;
- internações;
- leitos;
- equipamentos;
- profissionais;
- períodos de competência.

---

## API

A camada de serviço foi desenvolvida utilizando **FastAPI**.

A API realiza consultas ao Oracle Database e disponibiliza os dados necessários ao dashboard através de endpoints REST.

Entre os dados disponibilizados estão:

- quantidade de internações;
- permanência média;
- principais diagnósticos;
- hospitais com maior demanda;
- disponibilidade de leitos;
- disponibilidade de equipamentos;
- profissionais de saúde;
- informações das unidades;
- indicadores demográficos.

### API pública

```text
https://synthesus-api.onrender.com
```

### Documentação Swagger

```text
https://synthesus-api.onrender.com/docs
```

> A API utiliza uma instância gratuita no Render e pode apresentar um tempo maior de resposta na primeira requisição após um período de inatividade.

---

## Dashboard

O frontend do SyntheSUS foi desenvolvido utilizando:

- HTML;
- CSS;
- JavaScript;
- Bootstrap;
- Bootstrap Icons;
- Chart.js.

O dashboard é dividido nas seguintes áreas:

### Visão geral

Apresenta uma visão consolidada das informações de saúde, incluindo:

- total de internações;
- total de leitos;
- permanência média;
- quantidade de unidades;
- evolução das internações;
- hospitais com maior demanda;
- principais motivos de internação.

### Unidades

Permite analisar características e infraestrutura das unidades de saúde.

### Leitos

Apresenta informações sobre:

- leitos existentes;
- leitos disponíveis ao SUS;
- leitos contratados;
- hospitais com maior quantidade de leitos;
- distribuição por tipo de leito.

### Equipamentos

Permite analisar:

- equipamentos cadastrados;
- equipamentos em uso;
- equipamentos disponíveis ao SUS;
- distribuição por tipo;
- evolução ao longo do tempo.

### Profissionais

Disponibiliza indicadores relacionados a:

- profissionais cadastrados;
- profissionais vinculados ao SUS;
- carga horária;
- distribuição por ocupação.

### Internações

Permite analisar:

- quantidade de internações;
- permanência média;
- permanência em UTI;
- valor médio;
- diagnósticos;
- sexo;
- faixa etária;
- raça/cor;
- etnia.

---

## Filtros

O dashboard permite realizar diferentes recortes dos dados utilizando filtros como:

- período;
- região;
- UF;
- município;
- unidade de saúde;
- sexo;
- idade;
- raça/cor;
- etnia;
- caráter da internação.

Isso permite comparar indicadores em diferentes contextos geográficos e temporais.

---

## Tecnologias

### Engenharia de dados

- Python
- Pandas
- PyArrow
- Apache Airflow
- Docker
- ETL/ELT

### Cloud

- Oracle Cloud Infrastructure
- OCI Object Storage

### Banco de dados

- Oracle Database
- SQL

### Backend

- Python
- FastAPI
- Uvicorn
- python-oracledb

### Frontend

- HTML5
- CSS3
- JavaScript
- Bootstrap
- Bootstrap Icons
- Chart.js

### Versionamento e deploy

- Git
- GitHub
- Render

---

## Objetivo

O objetivo do SyntheSUS é reduzir a complexidade da consulta de diferentes fontes de dados de saúde pública e disponibilizar essas informações de maneira integrada e visual.

A solução busca auxiliar a análise de questões como:

- quais hospitais apresentam maior demanda;
- quais são os principais motivos de internação;
- como as internações evoluem ao longo do tempo;
- qual infraestrutura hospitalar está registrada em determinada região;
- quantos leitos estão disponíveis ao SUS;
- quais equipamentos e profissionais estão associados às unidades;
- quais grupos apresentam maior permanência hospitalar.

---

## Observação sobre disponibilidade de leitos

Os dados de leitos utilizados pelo SyntheSUS representam a **capacidade hospitalar registrada no CNES em determinada competência**.

Portanto, os valores não devem ser interpretados como disponibilidade de leitos em tempo real.

---

## Segurança

Credenciais e arquivos sensíveis não são armazenados neste repositório.

Arquivos como:

```text
.env
Wallet_*
*.pem
*.p12
*.sso
```

são excluídos do versionamento através do `.gitignore`.

As credenciais utilizadas pelas aplicações são fornecidas por meio de variáveis de ambiente e arquivos secretos configurados diretamente nos ambientes de execução.

---

## Projeto acadêmico

O SyntheSUS foi desenvolvido como projeto acadêmico do curso de **Data Science da FIAP**, aplicando conceitos de:

- engenharia de dados;
- bancos de dados relacionais;
- cloud computing;
- APIs;
- modelagem de dados;
- análise de dados;
- visualização de informações.

---

## Autor

**Gabriel Renan Maia da Silva**

Data Science — FIAP
