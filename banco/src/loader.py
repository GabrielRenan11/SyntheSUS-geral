import hashlib
import re

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


# ============================================================
# CONSTANTES DE HASH
# ============================================================

HASH_KEY_1 = "0123456789abcdef"
HASH_KEY_2 = "fedcba9876543210"
MASK_UINT64 = (1 << 64) - 1


# ============================================================
# VALIDAR IDENTIFICADORES ORACLE
# ============================================================

def validar_identificador(nome):
    if not re.fullmatch(
        r"[A-Za-z][A-Za-z0-9_]*",
        nome
    ):
        raise ValueError(
            f"Identificador Oracle inválido: {nome}"
        )

    return nome.upper()


# ============================================================
# OBTER SCHEMA ORACLE
# ============================================================

def obter_schema_oracle(cursor, tabela):
    tabela = validar_identificador(tabela)

    cursor.execute(
        """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            DATA_LENGTH,
            DATA_PRECISION,
            DATA_SCALE,
            NULLABLE,
            COLUMN_ID
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = :tabela
        ORDER BY COLUMN_ID
        """,
        tabela=tabela
    )

    colunas = cursor.fetchall()

    if not colunas:
        raise ValueError(
            f"Tabela {tabela} não encontrada no schema Oracle."
        )

    return [
        {
            "nome": coluna[0],
            "tipo": coluna[1],
            "tamanho": coluna[2],
            "precisao": coluna[3],
            "escala": coluna[4],
            "nullable": coluna[5],
            "ordem": coluna[6]
        }
        for coluna in colunas
    ]


# ============================================================
# CONVERTER VALORES PARA ORACLE
# ============================================================

def converter_valor(valor):
    if pd.isna(valor):
        return None

    if isinstance(valor, pd.Timestamp):
        return valor.to_pydatetime()

    if isinstance(valor, np.generic):
        return valor.item()

    return valor


# ============================================================
# NORMALIZAÇÃO DE COLUNAS GOLD -> ORACLE
# ============================================================

def normalizar_nome_coluna_para_oracle(
    tabela,
    coluna
):
    """
    Mantém a correção temporária do typo da Gold de ALVARA.
    """
    if (
        tabela.upper() == "ALVARA"
        and coluna.lower() == "unidae_saude_id_unidade_ver"
    ):
        return "unidade_saude_id_unidade_ver"

    return coluna


def obter_colunas_parquet_e_oracle(
    tabela,
    parquet
):
    colunas_parquet = list(
        parquet.schema.names
    )

    colunas_oracle = [
        normalizar_nome_coluna_para_oracle(
            tabela,
            coluna
        )
        for coluna in colunas_parquet
    ]

    return (
        colunas_parquet,
        colunas_oracle
    )


# ============================================================
# VALIDAR SCHEMA
# ============================================================

def validar_schema_parquet_oracle(
    cursor,
    tabela,
    colunas_oracle
):
    schema_oracle = obter_schema_oracle(
        cursor,
        tabela
    )

    nomes_oracle = {
        coluna["nome"].upper()
        for coluna in schema_oracle
    }

    nomes_gold = {
        coluna.upper()
        for coluna in colunas_oracle
    }

    extras = (
        nomes_gold - nomes_oracle
    )

    ausentes = (
        nomes_oracle - nomes_gold
    )

    if extras or ausentes:
        raise ValueError(
            f"Incompatibilidade de schema em {tabela}. "
            f"Extras no Parquet: {sorted(extras)} | "
            f"Ausentes no Parquet: {sorted(ausentes)}"
        )

    print(
        f"Schema {tabela}: OK"
    )


# ============================================================
# SQL DE INSERT
# ============================================================

def montar_sql_insert(
    tabela,
    colunas
):
    tabela = validar_identificador(
        tabela
    )

    colunas = [
        validar_identificador(
            coluna
        )
        for coluna in colunas
    ]

    placeholders = ", ".join(
        f":{i + 1}"
        for i in range(
            len(colunas)
        )
    )

    return f"""
        INSERT INTO {tabela} (
            {", ".join(colunas)}
        )
        VALUES (
            {placeholders}
        )
    """


# ============================================================
# SQL DE MERGE
# ============================================================

def montar_sql_merge(
    tabela,
    colunas,
    chaves
):
    tabela = validar_identificador(
        tabela
    )

    colunas = [
        validar_identificador(
            coluna
        )
        for coluna in colunas
    ]

    chaves = [
        validar_identificador(
            coluna
        )
        for coluna in chaves
    ]

    faltantes = (
        set(chaves) - set(colunas)
    )

    if faltantes:
        raise ValueError(
            f"Chave(s) ausente(s) no Parquet de {tabela}: "
            f"{sorted(faltantes)}"
        )

    select_origem = ", ".join(
        f":{i + 1} AS {coluna}"
        for i, coluna in enumerate(
            colunas
        )
    )

    condicao = " AND ".join(
        f"T.{chave} = S.{chave}"
        for chave in chaves
    )

    nao_chaves = [
        coluna
        for coluna in colunas
        if coluna not in chaves
    ]

    partes = [
        f"""
        MERGE INTO {tabela} T
        USING (
            SELECT {select_origem}
            FROM DUAL
        ) S
        ON ({condicao})
        """
    ]

    if nao_chaves:
        atualizacoes = ", ".join(
            f"T.{coluna} = S.{coluna}"
            for coluna in nao_chaves
        )

        partes.append(
            f"""
            WHEN MATCHED THEN
                UPDATE SET {atualizacoes}
            """
        )

    partes.append(
        f"""
        WHEN NOT MATCHED THEN
            INSERT (
                {", ".join(colunas)}
            )
            VALUES (
                {", ".join(f"S.{coluna}" for coluna in colunas)}
            )
        """
    )

    return "\n".join(
        partes
    )


# ============================================================
# DATAFRAME -> TUPLAS ORACLE
# ============================================================

def dataframe_para_tuplas(
    df
):
    return [
        tuple(
            converter_valor(
                valor
            )
            for valor in linha
        )
        for linha in df.itertuples(
            index=False,
            name=None
        )
    ]


# ============================================================
# INSERT DE DATAFRAME
# ============================================================

def inserir_dataframe(
    connection,
    tabela,
    df,
    batch_size=10000
):
    tabela = validar_identificador(
        tabela
    )

    sql = montar_sql_insert(
        tabela,
        df.columns
    )

    cursor = connection.cursor()

    try:
        total = len(df)

        for inicio in range(
            0,
            total,
            batch_size
        ):
            fim = min(
                inicio + batch_size,
                total
            )

            lote = df.iloc[
                inicio:fim
            ]

            dados = dataframe_para_tuplas(
                lote
            )

            cursor.executemany(
                sql,
                dados
            )

            print(
                f"{tabela}: {fim:,}/{total:,}"
            )

    finally:
        cursor.close()


# ============================================================
# CARGA COMPLETA - MANTIDA PARA COMPATIBILIDADE
# ============================================================

def carregar_parquet_em_batches(
    connection,
    tabela,
    caminho_parquet,
    batch_size=20000
):
    tabela = validar_identificador(
        tabela
    )

    parquet = pq.ParquetFile(
        caminho_parquet
    )

    total_parquet = (
        parquet.metadata.num_rows
    )

    (
        colunas_parquet,
        colunas_oracle
    ) = obter_colunas_parquet_e_oracle(
        tabela,
        parquet
    )

    cursor = connection.cursor()

    try:
        validar_schema_parquet_oracle(
            cursor,
            tabela,
            colunas_oracle
        )

        sql = montar_sql_insert(
            tabela,
            colunas_oracle
        )

        inseridos = 0

        for record_batch in parquet.iter_batches(
            batch_size=batch_size,
            columns=colunas_parquet
        ):
            df = record_batch.to_pandas()

            dados = dataframe_para_tuplas(
                df
            )

            cursor.executemany(
                sql,
                dados
            )

            inseridos += len(
                dados
            )

            print(
                f"{tabela}: "
                f"{inseridos:,}/{total_parquet:,}"
            )

        print(
            f"{tabela}: todos os batches enviados."
        )

        return total_parquet

    finally:
        cursor.close()


# ============================================================
# EXTRAIR COMPETÊNCIA DO DATAFRAME
# ============================================================

def extrair_competencia_dataframe(
    df,
    config
):
    if config.get(
        "coluna_competencia"
    ):
        coluna = config[
            "coluna_competencia"
        ]

        if coluna not in df.columns:
            raise ValueError(
                f"Coluna de competência '{coluna}' "
                "não encontrada no Parquet."
            )

        datas = pd.to_datetime(
            df[coluna],
            errors="coerce"
        )

        competencias = (
            datas.dt.strftime(
                "%Y%m"
            )
        )

    else:
        aliases = config.get(
            "colunas_competencia_origem",
            []
        )

        coluna = next(
            (
                nome
                for nome in aliases
                if nome in df.columns
            ),
            None
        )

        if coluna is None:
            raise ValueError(
                "Nenhuma coluna de origem da competência "
                f"foi encontrada. Candidatas: {aliases}"
            )

        serie = (
            df[coluna]
            .astype("string")
            .str.strip()
            .str.replace(
                r"\.0$",
                "",
                regex=True
            )
        )

        competencias = (
            serie
            .str.slice(
                0,
                6
            )
        )

        validas = competencias.str.fullmatch(
            r"\d{6}",
            na=False
        )

        competencias = competencias.where(
            validas
        )

    invalidas = int(
        competencias.isna().sum()
    )

    if invalidas > 0:
        raise ValueError(
            f"Foram encontradas {invalidas:,} linha(s) "
            "sem competência válida."
        )

    return competencias.astype(
        "string"
    )


# ============================================================
# HASH POR COMPETÊNCIA
# ============================================================

def analisar_parquet_por_competencia(
    caminho_parquet,
    config,
    batch_size=50000
):
    """
    Calcula um fingerprint independente da ordem das linhas.

    O hash final considera:
    - quantidade de linhas;
    - soma e XOR de dois hashes de linha diferentes.

    Isso permite detectar mudanças por competência sem carregar
    o Parquet inteiro na memória.
    """
    parquet = pq.ParquetFile(
        caminho_parquet
    )

    estados = {}

    lidas = 0
    total = parquet.metadata.num_rows

    for record_batch in parquet.iter_batches(
        batch_size=batch_size
    ):
        df = record_batch.to_pandas()

        competencias = (
            extrair_competencia_dataframe(
                df,
                config
            )
        )

        hash_1 = (
            pd.util.hash_pandas_object(
                df,
                index=False,
                hash_key=HASH_KEY_1
            )
            .to_numpy(
                dtype=np.uint64
            )
        )

        hash_2 = (
            pd.util.hash_pandas_object(
                df,
                index=False,
                hash_key=HASH_KEY_2
            )
            .to_numpy(
                dtype=np.uint64
            )
        )

        comp_array = competencias.to_numpy()

        for competencia in pd.unique(
            competencias
        ):
            mascara = (
                comp_array == competencia
            )

            valores_1 = hash_1[
                mascara
            ]

            valores_2 = hash_2[
                mascara
            ]

            estado = estados.setdefault(
                str(competencia),
                {
                    "qt_registros": 0,
                    "sum_1": 0,
                    "xor_1": 0,
                    "sum_2": 0,
                    "xor_2": 0
                }
            )

            estado["qt_registros"] += int(
                len(valores_1)
            )

            estado["sum_1"] = (
                estado["sum_1"]
                + int(
                    valores_1.sum(
                        dtype=np.uint64
                    )
                )
            ) & MASK_UINT64

            estado["sum_2"] = (
                estado["sum_2"]
                + int(
                    valores_2.sum(
                        dtype=np.uint64
                    )
                )
            ) & MASK_UINT64

            if len(valores_1) > 0:
                estado["xor_1"] ^= int(
                    np.bitwise_xor.reduce(
                        valores_1
                    )
                )

                estado["xor_2"] ^= int(
                    np.bitwise_xor.reduce(
                        valores_2
                    )
                )

        lidas += len(df)

        print(
            f"Hash {config['tabela']}: "
            f"{lidas:,}/{total:,}"
        )

    resultado = {}

    for competencia, estado in sorted(
        estados.items()
    ):
        assinatura = (
            f"{estado['qt_registros']}|"
            f"{estado['sum_1']:016x}|"
            f"{estado['xor_1']:016x}|"
            f"{estado['sum_2']:016x}|"
            f"{estado['xor_2']:016x}"
        )

        hash_final = hashlib.sha256(
            assinatura.encode(
                "utf-8"
            )
        ).hexdigest()

        resultado[competencia] = {
            "hash": hash_final,
            "qt_registros": estado[
                "qt_registros"
            ]
        }

    return resultado


# ============================================================
# CONTAGENS ORACLE POR COMPETÊNCIA
# ============================================================

def obter_contagens_oracle_por_competencia(
    connection,
    config
):
    tabela = validar_identificador(
        config["tabela"]
    )

    cursor = connection.cursor()

    try:
        if config.get(
            "coluna_competencia"
        ):
            coluna = validar_identificador(
                config[
                    "coluna_competencia"
                ]
            )

            cursor.execute(
                f"""
                SELECT
                    TO_CHAR({coluna}, 'YYYYMM') AS COMPETENCIA,
                    COUNT(*)
                FROM {tabela}
                WHERE {coluna} IS NOT NULL
                GROUP BY TO_CHAR({coluna}, 'YYYYMM')
                ORDER BY COMPETENCIA
                """
            )

        else:
            coluna_oracle = validar_identificador(
                config[
                    "coluna_competencia_oracle"
                ]
            )

            cursor.execute(
                f"""
                SELECT
                    SUBSTR(
                        TO_CHAR({coluna_oracle}),
                        1,
                        6
                    ) AS COMPETENCIA,
                    COUNT(*)
                FROM {tabela}
                WHERE {coluna_oracle} IS NOT NULL
                GROUP BY SUBSTR(
                    TO_CHAR({coluna_oracle}),
                    1,
                    6
                )
                ORDER BY COMPETENCIA
                """
            )

        return {
            str(competencia): int(
                quantidade
            )
            for competencia, quantidade
            in cursor.fetchall()
        }

    finally:
        cursor.close()


# ============================================================
# DELETE DE UMA OU MAIS COMPETÊNCIAS
# ============================================================

def deletar_competencias(
    connection,
    config,
    competencias
):
    tabela = validar_identificador(
        config["tabela"]
    )

    competencias = sorted(
        set(competencias)
    )

    if not competencias:
        return 0

    cursor = connection.cursor()

    total = 0

    try:
        if config.get(
            "coluna_competencia"
        ):
            coluna = validar_identificador(
                config[
                    "coluna_competencia"
                ]
            )

            sql = f"""
                DELETE FROM {tabela}
                WHERE {coluna} = :competencia
            """

            for competencia in competencias:
                data_competencia = pd.Timestamp(
                    f"{competencia[:4]}-"
                    f"{competencia[4:6]}-01"
                ).to_pydatetime()

                cursor.execute(
                    sql,
                    competencia=data_competencia
                )

                quantidade = cursor.rowcount
                total += quantidade

                print(
                    f"{tabela}: DELETE {competencia} "
                    f"-> {quantidade:,} linha(s)"
                )

        else:
            coluna = validar_identificador(
                config[
                    "coluna_competencia_oracle"
                ]
            )

            sql = f"""
                DELETE FROM {tabela}
                WHERE SUBSTR(
                    TO_CHAR({coluna}),
                    1,
                    6
                ) = :competencia
            """

            for competencia in competencias:
                cursor.execute(
                    sql,
                    competencia=competencia
                )

                quantidade = cursor.rowcount
                total += quantidade

                print(
                    f"{tabela}: DELETE {competencia} "
                    f"-> {quantidade:,} linha(s)"
                )

        return total

    finally:
        cursor.close()


# ============================================================
# INSERT APENAS DAS COMPETÊNCIAS ALTERADAS
# ============================================================

def inserir_competencias_em_batches(
    connection,
    config,
    caminho_parquet,
    competencias,
    batch_size=20000
):
    tabela = validar_identificador(
        config["tabela"]
    )

    competencias = {
        str(comp)
        for comp in competencias
    }

    if not competencias:
        return {}

    parquet = pq.ParquetFile(
        caminho_parquet
    )

    (
        colunas_parquet,
        colunas_oracle
    ) = obter_colunas_parquet_e_oracle(
        tabela,
        parquet
    )

    cursor = connection.cursor()

    inseridos = {
        competencia: 0
        for competencia in competencias
    }

    try:
        validar_schema_parquet_oracle(
            cursor,
            tabela,
            colunas_oracle
        )

        sql = montar_sql_insert(
            tabela,
            colunas_oracle
        )

        for record_batch in parquet.iter_batches(
            batch_size=batch_size,
            columns=colunas_parquet
        ):
            df = record_batch.to_pandas()

            comp = extrair_competencia_dataframe(
                df,
                config
            )

            mascara = comp.isin(
                competencias
            )

            if not mascara.any():
                continue

            df_filtrado = df.loc[
                mascara
            ].copy()

            comp_filtrado = comp.loc[
                mascara
            ]

            dados = dataframe_para_tuplas(
                df_filtrado
            )

            cursor.executemany(
                sql,
                dados
            )

            contagens = (
                comp_filtrado
                .value_counts()
                .to_dict()
            )

            for competencia, quantidade in contagens.items():
                inseridos[
                    str(competencia)
                ] += int(
                    quantidade
                )

            print(
                f"{tabela}: "
                f"{sum(inseridos.values()):,} "
                "linha(s) nova(s) enviadas"
            )

        return inseridos

    finally:
        cursor.close()


# ============================================================
# MERGE APENAS DAS COMPETÊNCIAS ALTERADAS
# ============================================================

def merge_competencias_em_batches(
    connection,
    config,
    caminho_parquet,
    competencias,
    batch_size=10000
):
    tabela = validar_identificador(
        config["tabela"]
    )

    competencias = {
        str(comp)
        for comp in competencias
    }

    if not competencias:
        return {}

    parquet = pq.ParquetFile(
        caminho_parquet
    )

    (
        colunas_parquet,
        colunas_oracle
    ) = obter_colunas_parquet_e_oracle(
        tabela,
        parquet
    )

    cursor = connection.cursor()

    processados = {
        competencia: 0
        for competencia in competencias
    }

    try:
        validar_schema_parquet_oracle(
            cursor,
            tabela,
            colunas_oracle
        )

        sql = montar_sql_merge(
            tabela=tabela,
            colunas=colunas_oracle,
            chaves=config["chave"]
        )

        for record_batch in parquet.iter_batches(
            batch_size=batch_size,
            columns=colunas_parquet
        ):
            df = record_batch.to_pandas()

            comp = extrair_competencia_dataframe(
                df,
                config
            )

            mascara = comp.isin(
                competencias
            )

            if not mascara.any():
                continue

            df_filtrado = df.loc[
                mascara
            ].copy()

            comp_filtrado = comp.loc[
                mascara
            ]

            dados = dataframe_para_tuplas(
                df_filtrado
            )

            cursor.executemany(
                sql,
                dados
            )

            contagens = (
                comp_filtrado
                .value_counts()
                .to_dict()
            )

            for competencia, quantidade in contagens.items():
                processados[
                    str(competencia)
                ] += int(
                    quantidade
                )

            print(
                f"{tabela}: "
                f"{sum(processados.values()):,} "
                "linha(s) processadas por MERGE"
            )

        return processados

    finally:
        cursor.close()


# ============================================================
# MERGE DE DIMENSÃO INTEIRA
# ============================================================

def merge_parquet_em_batches(
    connection,
    config,
    caminho_parquet,
    batch_size=10000
):
    tabela = validar_identificador(
        config["tabela"]
    )

    parquet = pq.ParquetFile(
        caminho_parquet
    )

    total = parquet.metadata.num_rows

    (
        colunas_parquet,
        colunas_oracle
    ) = obter_colunas_parquet_e_oracle(
        tabela,
        parquet
    )

    cursor = connection.cursor()

    processados = 0

    try:
        validar_schema_parquet_oracle(
            cursor,
            tabela,
            colunas_oracle
        )

        sql = montar_sql_merge(
            tabela=tabela,
            colunas=colunas_oracle,
            chaves=config["chave"]
        )

        for record_batch in parquet.iter_batches(
            batch_size=batch_size,
            columns=colunas_parquet
        ):
            df = record_batch.to_pandas()

            dados = dataframe_para_tuplas(
                df
            )

            cursor.executemany(
                sql,
                dados
            )

            processados += len(
                dados
            )

            print(
                f"{tabela}: MERGE "
                f"{processados:,}/{total:,}"
            )

        return total

    finally:
        cursor.close()


# ============================================================
# RETENÇÃO DE 12 MESES
# ============================================================

def remover_fora_retencao(
    connection,
    config,
    limite
):
    tabela = validar_identificador(
        config["tabela"]
    )

    if not config.get(
        "retencao",
        False
    ):
        return 0

    cursor = connection.cursor()

    try:
        if config.get(
            "coluna_competencia"
        ):
            coluna = validar_identificador(
                config[
                    "coluna_competencia"
                ]
            )

            cursor.execute(
                f"""
                DELETE FROM {tabela}
                WHERE {coluna} < :limite
                """,
                limite=limite
            )

        else:
            coluna = validar_identificador(
                config[
                    "coluna_competencia_oracle"
                ]
            )

            limite_yyyymm = int(
                limite.strftime(
                    "%Y%m"
                )
            )

            cursor.execute(
                f"""
                DELETE FROM {tabela}
                WHERE TO_NUMBER(
                    SUBSTR(
                        TO_CHAR({coluna}),
                        1,
                        6
                    )
                ) < :limite_yyyymm
                """,
                limite_yyyymm=limite_yyyymm
            )

        quantidade = cursor.rowcount

        print(
            f"{tabela}: retenção removeu "
            f"{quantidade:,} linha(s)."
        )

        return quantidade

    finally:
        cursor.close()