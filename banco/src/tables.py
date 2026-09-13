TABELAS = [
    {
        "arquivo": "DIM_DATA.parquet",
        "tabela": "DIM_DATA",
        "batch_size": 10000,
        "estrategia": "merge_dimensao",
        "chave": ["data"],
        "retencao": False,
    },
    {
        "arquivo": "DIM_MUNICIPIO.parquet",
        "tabela": "DIM_MUNICIPIO",
        "batch_size": 10000,
        "estrategia": "merge_dimensao",
        "chave": ["id_municipio"],
        "retencao": False,
    },
    {
        "arquivo": "UNIDADE_SAUDE.parquet",
        "tabela": "UNIDADE_SAUDE",
        "batch_size": 10000,
        "estrategia": "merge_competencia",
        "coluna_competencia": "dim_data_data",
        "chave": ["id_versao_unidade"],
        "retencao": True,
    },
    {
        "arquivo": "ALVARA.parquet",
        "tabela": "ALVARA",
        "batch_size": 10000,
        "estrategia": "replace_competencia",
        "colunas_competencia_origem": [
            "unidae_saude_id_unidade_ver",
            "unidade_saude_id_unidade_ver",
        ],
        "coluna_competencia_oracle":
            "unidade_saude_id_unidade_ver",
        "retencao": True,
    },
    {
        "arquivo": "PROFISSIONAL.parquet",
        "tabela": "PROFISSIONAL",
        "batch_size": 10000,
        "estrategia": "replace_competencia",
        "coluna_competencia": "dim_data_data",
        "retencao": True,
    },
    {
        "arquivo": "FATO_LEITOS.parquet",
        "tabela": "FATO_LEITOS",
        "batch_size": 20000,
        "estrategia": "replace_competencia",
        "coluna_competencia": "dim_data_data",
        "retencao": True,
    },
    {
        "arquivo": "FATO_EQUIPAMENTOS.parquet",
        "tabela": "FATO_EQUIPAMENTOS",
        "batch_size": 20000,
        "estrategia": "replace_competencia",
        "coluna_competencia": "dim_data_data",
        "retencao": True,
    },
    {
        "arquivo": "INTERNACAO.parquet",
        "tabela": "INTERNACAO",
        "batch_size": 20000,
        "estrategia": "replace_competencia",
        "coluna_competencia":
            "dim_data_data_competencia",
        "retencao": True,
    },
]


TABELAS_POR_NOME = {
    item["tabela"]: item
    for item in TABELAS
}