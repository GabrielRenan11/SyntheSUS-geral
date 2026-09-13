# SyntheSUS

## Sobre

O **SyntheSUS** é um dashboard de saúde desenvolvido para facilitar a análise e a interpretação de dados provenientes dos sistemas do **DATASUS**, com foco especial em internações hospitalares do **SIH/SUS**.

A solução permite analisar e comparar diferentes indicadores relacionados a internações e à infraestrutura de saúde, incluindo:

* quantidade e disponibilidade de leitos hospitalares;
* internações por unidade, município e região;
* sexo, raça/cor e idade dos pacientes;
* município de residência e de internação;
* duração das internações;
* custos hospitalares;
* profissionais disponíveis;
* equipamentos e infraestrutura das unidades de saúde.

O objetivo é transformar grandes volumes de dados públicos de saúde em informações mais acessíveis para análise e apoio à tomada de decisão.

---

## About

**SyntheSUS** is a healthcare analytics dashboard designed to simplify the analysis and interpretation of data from **DATASUS**, with a particular focus on hospital admissions from Brazil's **SIH/SUS** system.

The platform enables users to explore and compare healthcare indicators related to hospital admissions and healthcare infrastructure, including:

* hospital bed availability;
* admissions by healthcare facility, municipality, and region;
* patient characteristics such as sex, race/ethnicity, and age;
* municipality of residence and hospitalization;
* length of stay;
* hospitalization costs;
* healthcare professionals;
* medical equipment and infrastructure.

The project aims to transform large volumes of Brazilian public healthcare data into accessible information for analysis and decision-making.

---

## Tecnologias / Technologies

* Python
* SQL
* Apache Airflow
* Docker
* Oracle Cloud Infrastructure (OCI)
* Pandas
* Parquet
* DATASUS / SIH-SUS

---

## Arquitetura/Architecture

**DATASUS → Ingestão → Bronze → Silver → Gold → Dashboard**

PT: Os processos de ingestão e transformação são executados em containers Docker e orquestrados utilizando Apache Airflow.
EN: The ingestion and transform processes are executed in Docker containers and orchestrated via Apache Airflow.

---

## Dados

Os dados utilizados pelo projeto são provenientes de bases públicas disponibilizadas pelo **DATASUS**, incluindo informações do **CNES** e do **SIH/SUS**.

---

## Status

🚧 **Projeto em desenvolvimento**
<details>
Seguinte, faça os seguintes passos no VSCode antes de executar.

1. Colocar o arquivo . env:
AIRFLOW_UID=50000
FERNET_KEY=<n vou por aqui>

2. Baixar o arquivo .pem e jogar na pasta principal
 
3. Rodar o comando 'build' na pasta SyntheSUS:
docker build -t synthesus-ingestao:latest .
OU
docker build -t synthesus-transformacao:latest .
OU
docker build -t synthesus-gold:latest .

4. Selecionar a pasta "airflow" e finalmente executar.
</details>
