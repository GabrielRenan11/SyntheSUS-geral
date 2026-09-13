"""
SyntheSUS - Transformação GOLD para Docker/Airflow

Base:
- processamento mensal das partições Silver;
- escrita unificada via PyArrow/ParquetWriter;
- schemas Gold fixos;
- enriquecimentos produzidos pela Silver atual são preservados;
- nomes de colunas alinhados ao schema físico usado pela API/Oracle.

Variáveis de ambiente esperadas:
- OCI_CONFIG_FILE (opcional; padrão: "config")
- BUCKET_SILVER   (opcional; padrão: "synthesus-silver")
- BUCKET_GOLD     (opcional; padrão: "synthesus-gold")
"""

import gc
import io
import os
import re
import tempfile

import oci
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# ==========================================================
# CONFIGURAÇÕES
# ==========================================================

# Configuração própria para execução local/container/Airflow.
# No Docker/Airflow, defina OCI_CONFIG_FILE quando o arquivo não estiver
# no caminho padrão "config".
PATH_CONFIG = os.getenv(
    "OCI_CONFIG_FILE",
    "config",
)

BUCKET_SILVER = os.getenv(
    "BUCKET_SILVER",
    "synthesus-silver",
)

BUCKET_GOLD = os.getenv(
    "BUCKET_GOLD",
    "synthesus-gold",
)


# ==========================================================
# SCHEMAS GOLD
#
# IMPORTANTE:
# Os nomes abaixo seguem EXATAMENTE a estrutura solicitada.
# ==========================================================

SCHEMA_DATA = pa.schema([
    pa.field("data", pa.timestamp("ns")),
    pa.field("mes", pa.string()),
    pa.field("ano", pa.string()),
    pa.field("dia_ano", pa.string()),
    pa.field("dia_mes", pa.string()),
    pa.field("semana_mes", pa.string()),
    pa.field("semana_ano", pa.string()),
    pa.field("dia_semana_nome", pa.string()),
    pa.field("mes_nome", pa.string()),
    pa.field("bimestre", pa.string()),
    pa.field("trimestre", pa.string()),
    pa.field("semestre", pa.string()),
])

SCHEMA_MUNICIPIO = pa.schema([
    pa.field("id_municipio", pa.string()),
    pa.field("nome_municipio", pa.string()),
    pa.field("regiao", pa.string()),
    pa.field("uf", pa.string()),
])

SCHEMA_EQUIPAMENTOS = pa.schema([
    pa.field("id_equipamento", pa.string()),
    pa.field("dim_data_data", pa.timestamp("ns")),
    pa.field("qt_equipamentos", pa.float64()),
    pa.field("qt_uso", pa.float64()),
    pa.field("disponibilidade_sus", pa.string()),
    pa.field("dim_municipio_id_municipio", pa.string()),
    pa.field("unidade_saude_id_unidade_ver", pa.string()),
    pa.field("tipo_equipamento", pa.string()),
    pa.field("codigo_equipamento", pa.string()),
])

SCHEMA_LEITOS = pa.schema([
    pa.field("id_leitos", pa.string()),
    pa.field("dim_data_data", pa.timestamp("ns")),
    pa.field("qt_contratada", pa.float64()),
    pa.field("disponivel_sus", pa.float64()),
    pa.field("qt_existente", pa.float64()),
    pa.field("tipo_leito", pa.string()),
    pa.field("dim_municipio_id_municipio", pa.string()),
    pa.field("unidade_saude_id_unidade_ver", pa.string()),
])

SCHEMA_ALVARA = pa.schema([
    pa.field("id_alvara", pa.int64()),
    pa.field("alvara", pa.string()),
    pa.field("data_emissao", pa.timestamp("ns")),
    # Nome mantido exatamente como solicitado.
    pa.field("unidade_saude_id_unidade_ver", pa.string()),
])

SCHEMA_INTERNACAO = pa.schema([
    pa.field("id_internacao", pa.string()),
    pa.field("diagnostico", pa.string()),
    pa.field("valor_internacao", pa.float64()),
    pa.field("idade", pa.float64()),
    pa.field("sexo", pa.string()),
    pa.field("dias_uti", pa.float64()),
    pa.field("carater_internacao", pa.string()),
    pa.field("raca_cor", pa.string()),
    pa.field("etnia", pa.string()),
    pa.field("tipo_uti", pa.string()),
    pa.field("dim_data_data_competencia", pa.timestamp("ns")),
    pa.field("dim_data_data_entrada", pa.timestamp("ns")),
    pa.field("dim_data_data_saida", pa.timestamp("ns")),
    pa.field("dim_municipio_municipio_res", pa.string()),
    pa.field("dim_municipio_municipio_int", pa.string()),
    # Nome mantido exatamente como solicitado.
    # O VALOR armazenado é a chave da foto da unidade: YYYYMM + CNES.
    pa.field("unidade_saude_id_unidade", pa.string()),
])

SCHEMA_UNIDADE = pa.schema([
    pa.field("id_versao_unidade", pa.string()),
    pa.field("id_unidade_saude", pa.string()),
    pa.field("dim_data_data", pa.timestamp("ns")),
    pa.field("tipo_unidade", pa.string()),
    pa.field("clientela", pa.string()),
    pa.field("dim_municipio_id_municipio", pa.string()),
])

SCHEMA_PROFISSIONAL = pa.schema([
    pa.field("id_profissional", pa.int64()),
    pa.field("dim_data_data", pa.timestamp("ns")),
    pa.field("horas_trabalhadas", pa.float64()),
    pa.field("vinculo_sus", pa.string()),
    pa.field("cbo", pa.string()),
    pa.field("registro", pa.string()),
    pa.field("dim_municipio_id_municipio", pa.string()),
    pa.field("unidade_saude_id_unidade_ver", pa.string()),
    pa.field("cns", pa.string()),
])


# ==========================================================
# OCI
# ==========================================================

def obter_cliente_oci():
    try:
        config = oci.config.from_file(
            PATH_CONFIG,
            "DEFAULT",
        )

        client = oci.object_storage.ObjectStorageClient(
            config
        )

        namespace = client.get_namespace().data

        print("✓ Conectado à OCI com sucesso!")
        print(f"✓ Silver: {BUCKET_SILVER}")
        print(f"✓ Gold:   {BUCKET_GOLD}")

        return client, namespace

    except Exception as e:
        print(
            f"❌ Erro ao autenticar na OCI usando "
            f"'{PATH_CONFIG}': {e}"
        )
        raise


# ==========================================================
# LISTAGEM DE OBJETOS
# ==========================================================

def listar_objetos_oci(
    client,
    namespace,
    bucket,
    prefixo,
):
    """Lista todos os objetos de um prefixo, com paginação."""
    objetos = []
    start = None

    while True:
        parametros = {
            "namespace_name": namespace,
            "bucket_name": bucket,
            "prefix": prefixo,
        }

        if start is not None:
            parametros["start"] = start

        resposta = client.list_objects(**parametros)
        objetos.extend(resposta.data.objects)

        if not resposta.data.next_start_with:
            break

        start = resposta.data.next_start_with

    return objetos


def listar_particoes_competencia(
    client,
    namespace,
    bucket,
    prefixo_base,
):
    """
    Localiza somente:
        <prefixo_base>YYYYMM.parquet
    """
    padrao = re.compile(
        rf"^{re.escape(prefixo_base)}(\d{{6}})\.parquet$"
    )

    encontrados = []

    for obj in listar_objetos_oci(
        client,
        namespace,
        bucket,
        prefixo_base,
    ):
        match = padrao.match(obj.name)

        if not match:
            continue

        competencia = match.group(1)
        ano = int(competencia[:4])
        mes = int(competencia[4:6])

        if ano < 1900 or mes < 1 or mes > 12:
            continue

        encontrados.append(
            (competencia, obj.name)
        )

    encontrados.sort(key=lambda item: item[0])

    if not encontrados:
        raise FileNotFoundError(
            f"Nenhuma partição "
            f"'{prefixo_base}YYYYMM.parquet' "
            f"foi encontrada em '{bucket}'."
        )

    print(
        f"✓ {len(encontrados)} partição(ões) encontrada(s) "
        f"para '{prefixo_base}'."
    )
    print(
        "  Competências: "
        + ", ".join(comp for comp, _ in encontrados)
    )

    return encontrados


# ==========================================================
# LEITURA OCI
# ==========================================================

def ler_parquet_oci(
    client,
    namespace,
    bucket,
    object_name,
):
    print(
        f"📥 Lendo '{object_name}' "
        f"do bucket '{bucket}'..."
    )

    response = client.get_object(
        namespace_name=namespace,
        bucket_name=bucket,
        object_name=object_name,
    )

    return pd.read_parquet(
        io.BytesIO(response.data.content)
    )


def ler_parquet_oci_pyarrow(
    client,
    namespace,
    bucket,
    object_name,
    colunas=None,
):
    """
    Leitura via PyArrow, especialmente útil para PF.
    """
    print(
        f"📥 Lendo '{object_name}' via PyArrow "
        f"do bucket '{bucket}'..."
    )

    response = client.get_object(
        namespace_name=namespace,
        bucket_name=bucket,
        object_name=object_name,
    )

    buffer = io.BytesIO(response.data.content)
    parquet_file = pq.ParquetFile(buffer)

    if colunas is not None:
        existentes = set(parquet_file.schema.names)
        colunas = [
            coluna
            for coluna in colunas
            if coluna in existentes
        ]

    return parquet_file.read(
        columns=colunas
    ).to_pandas()


# ==========================================================
# HELPERS
# ==========================================================

def garantir_colunas(df, colunas):
    for coluna in colunas:
        if coluna not in df.columns:
            df[coluna] = None
    return df


def texto(serie):
    return (
        serie
        .astype("string")
        .str.strip()
    )


def codigo(serie, largura=None):
    resultado = texto(serie)

    resultado = resultado.str.replace(
        r"\.0$",
        "",
        regex=True,
    )

    if largura is not None:
        mascara = resultado.str.fullmatch(
            r"\d+",
            na=False,
        )

        resultado.loc[mascara] = (
            resultado.loc[mascara]
            .str.zfill(largura)
        )

    return resultado


def competencia_para_chave(serie_competen):
    return (
        pd.to_datetime(
            serie_competen,
            errors="coerce",
        )
        .dt.strftime("%Y%m")
    )


def montar_id_versao_unidade(
    serie_competen,
    serie_cnes,
    competencia_fallback=None,
):
    """
    Chave da FOTO da unidade:
        YYYYMM + CNES

    Exemplo:
        COMPETEN = 2026-06-01
        CNES     = 8239592
        resultado = 2026068239592
    """
    competencia_chave = competencia_para_chave(
        serie_competen
    )

    if competencia_fallback is not None:
        competencia_chave = competencia_chave.fillna(
            competencia_fallback
        )

    cnes = codigo(
        serie_cnes,
        7,
    )

    return competencia_chave + cnes


def combinar_codigo_descricao(
    serie_codigo,
    serie_descricao,
    separador=" - ",
):
    """
    Une código e descrição sem criar strings como '<NA>'.

    Se houver ambos:
        CODIGO + separador + DESCRICAO

    Se só houver código:
        CODIGO

    Se só houver descrição:
        DESCRICAO
    """
    cod = texto(serie_codigo)
    desc = texto(serie_descricao)

    cod_valido = (
        cod.notna()
        & cod.ne("")
        & ~cod.str.lower().isin(
            ["nan", "none", "<na>", "null"]
        )
    )

    desc_valida = (
        desc.notna()
        & desc.ne("")
        & ~desc.str.lower().isin(
            ["nan", "none", "<na>", "null"]
        )
    )

    resultado = pd.Series(
        pd.NA,
        index=serie_codigo.index,
        dtype="string",
    )

    ambos = cod_valido & desc_valida

    resultado.loc[ambos] = (
        cod.loc[ambos]
        + separador
        + desc.loc[ambos]
    )

    resultado.loc[cod_valido & ~desc_valida] = (
        cod.loc[cod_valido & ~desc_valida]
    )

    resultado.loc[~cod_valido & desc_valida] = (
        desc.loc[~cod_valido & desc_valida]
    )

    return resultado


def reordenar_e_selecionar(
    df,
    colunas_desejadas,
):
    garantir_colunas(
        df,
        colunas_desejadas,
    )

    return df[
        colunas_desejadas
    ].copy()


# ==========================================================
# DATAFRAME -> PYARROW COM SCHEMA FIXO
# ==========================================================

def dataframe_para_table(df, schema):
    """
    Força exatamente o schema Gold solicitado.
    """
    df = reordenar_e_selecionar(
        df,
        schema.names,
    )

    arrays = []

    for field in schema:
        serie = df[field.name]

        if pa.types.is_string(field.type):
            serie = serie.astype("string")

        elif pa.types.is_timestamp(field.type):
            serie = pd.to_datetime(
                serie,
                errors="coerce",
            )

        elif pa.types.is_int64(field.type):
            serie = pd.to_numeric(
                serie,
                errors="coerce",
            ).astype("Int64")

        elif pa.types.is_float64(field.type):
            serie = pd.to_numeric(
                serie,
                errors="coerce",
            ).astype("float64")

        arrays.append(
            pa.array(
                serie,
                type=field.type,
                from_pandas=True,
            )
        )

    return pa.Table.from_arrays(
        arrays,
        schema=schema,
    )


# ==========================================================
# SALVAR UM DATAFRAME NA GOLD
# ==========================================================

def salvar_parquet_oci(
    client,
    namespace,
    bucket,
    object_name,
    df,
    schema=None,
):
    print(
        f"📤 Salvando '{object_name}' "
        f"em '{bucket}' "
        f"({len(df):,} registros)..."
    )

    buffer = io.BytesIO()

    try:
        if schema is None:
            df.to_parquet(
                buffer,
                index=False,
                engine="pyarrow",
            )

        else:
            tabela = dataframe_para_table(
                df,
                schema,
            )

            pq.write_table(
                tabela,
                buffer,
                compression="snappy",
            )

        buffer.seek(0)

        client.put_object(
            namespace_name=namespace,
            bucket_name=bucket,
            object_name=object_name,
            put_object_body=buffer,
        )

    finally:
        buffer.close()

    print(
        f"✓ '{object_name}' salvo na synthesus-gold.\n"
    )


# ==========================================================
# UNIFICAR PARTIÇÕES SILVER EM UM ÚNICO PARQUET GOLD
# ==========================================================

def construir_gold_unificado(
    client,
    namespace,
    prefixo_silver,
    object_name_gold,
    transformar,
    schema_gold,
    usar_pyarrow=False,
    colunas_leitura=None,
):
    """
    Lê uma partição Silver por vez e escreve um único Parquet Gold.

    Isso evita carregar os 12 meses inteiros simultaneamente na RAM.
    """
    particoes = listar_particoes_competencia(
        client,
        namespace,
        BUCKET_SILVER,
        prefixo_silver,
    )

    temp = tempfile.NamedTemporaryFile(
        prefix=object_name_gold.replace(
            ".parquet",
            "",
        ) + "_",
        suffix=".parquet",
        delete=False,
    )

    caminho_temp = temp.name
    temp.close()

    if os.path.exists(caminho_temp):
        os.remove(caminho_temp)

    writer = None
    total_linhas = 0
    total_particoes = 0

    try:
        for competencia, object_name in particoes:
            print(
                f"\n🧩 {object_name_gold}: "
                f"competência {competencia}"
            )

            if usar_pyarrow:
                df = ler_parquet_oci_pyarrow(
                    client,
                    namespace,
                    BUCKET_SILVER,
                    object_name,
                    colunas=colunas_leitura,
                )
            else:
                df = ler_parquet_oci(
                    client,
                    namespace,
                    BUCKET_SILVER,
                    object_name,
                )

            if df.empty:
                print(
                    f"⚠️ {object_name}: vazio. Ignorando."
                )
                del df
                gc.collect()
                continue

            df_gold = transformar(
                df,
                competencia,
            )

            if df_gold.empty:
                print(
                    f"⚠️ {competencia}: transformação "
                    "resultou em zero linhas."
                )
                del df, df_gold
                gc.collect()
                continue

            tabela = dataframe_para_table(
                df_gold,
                schema_gold,
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    caminho_temp,
                    schema_gold,
                    compression="snappy",
                )

            writer.write_table(tabela)

            total_linhas += len(df_gold)
            total_particoes += 1

            print(
                f"✓ {competencia}: "
                f"{len(df_gold):,} linhas adicionadas."
            )

            del df, df_gold, tabela
            gc.collect()

        if writer is None:
            raise ValueError(
                f"Nenhuma linha produzida para "
                f"'{object_name_gold}'."
            )

        writer.close()
        writer = None

        parquet = pq.ParquetFile(
            caminho_temp
        )

        if parquet.metadata.num_rows != total_linhas:
            raise ValueError(
                f"Falha de validação em "
                f"'{object_name_gold}': "
                f"esperado {total_linhas:,}, "
                f"encontrado "
                f"{parquet.metadata.num_rows:,}."
            )

        # Confere também nomes e ordem das colunas.
        nomes_gravados = (
            parquet.schema_arrow.names
        )

        if nomes_gravados != schema_gold.names:
            raise ValueError(
                f"Schema incorreto em "
                f"'{object_name_gold}'.\n"
                f"Esperado: {schema_gold.names}\n"
                f"Obtido:   {nomes_gravados}"
            )

        print(
            f"🔎 '{object_name_gold}' validado: "
            f"{total_particoes} competência(s), "
            f"{total_linhas:,} linhas."
        )

        print(
            f"📤 Enviando '{object_name_gold}' "
            f"para '{BUCKET_GOLD}'..."
        )

        with open(
            caminho_temp,
            "rb",
        ) as arquivo:
            client.put_object(
                namespace_name=namespace,
                bucket_name=BUCKET_GOLD,
                object_name=object_name_gold,
                put_object_body=arquivo,
            )

        print(
            f"✅ '{object_name_gold}' concluído.\n"
        )

    finally:
        if writer is not None:
            writer.close()

        if os.path.exists(caminho_temp):
            os.remove(caminho_temp)

        gc.collect()


# ==========================================================
# UNIDADE_SAUDE
# ==========================================================

def transformar_unidade(
    df,
    competencia,
):
    colunas = [
        "CNES",
        "TP_UNID",
        "CODUFMUN",
        "CLIENTEL",
        "COMPETEN",
    ]

    df = garantir_colunas(
        df.copy(),
        colunas,
    )

    df["id_versao_unidade"] = (
        montar_id_versao_unidade(
            df["COMPETEN"],
            df["CNES"],
            competencia,
        )
    )

    df["id_unidade_saude"] = codigo(
        df["CNES"],
        7,
    )

    df["dim_data_data"] = pd.to_datetime(
        df["COMPETEN"],
        errors="coerce",
    )

    df["tipo_unidade"] = texto(
        df["TP_UNID"]
    )

    df["clientela"] = texto(
        df["CLIENTEL"]
    )

    df["dim_municipio_id_municipio"] = codigo(
        df["CODUFMUN"],
        6,
    )

    return reordenar_e_selecionar(
        df,
        SCHEMA_UNIDADE.names,
    )


# ==========================================================
# FATO_EQUIPAMENTOS
# ==========================================================

def transformar_equipamentos(
    df,
    competencia,
):
    colunas = [
        "CNES",
        "CODUFMUN",
        "TIPEQUIP",
        "TIPO_EQUIPAMENTO",
        "CODEQUIP",
        "DESC_EQUIPAMENTO",
        "QT_EXIST",
        "QT_USO",
        "COMPETEN",
        "IND_SUS",
    ]

    df = garantir_colunas(
        df.copy(),
        colunas,
    )

    competencia_chave = (
        competencia_para_chave(
            df["COMPETEN"]
        )
        .fillna(competencia)
    )

    cnes = codigo(
        df["CNES"],
        7,
    )

    df["unidade_saude_id_unidade_ver"] = (
        competencia_chave
        + cnes
    )

    df["dim_data_data"] = pd.to_datetime(
        df["COMPETEN"],
        errors="coerce",
    )

    df["qt_equipamentos"] = pd.to_numeric(
        df["QT_EXIST"],
        errors="coerce",
    )

    df["qt_uso"] = pd.to_numeric(
        df["QT_USO"],
        errors="coerce",
    )

    df["disponibilidade_sus"] = texto(
        df["IND_SUS"]
    )

    df["dim_municipio_id_municipio"] = codigo(
        df["CODUFMUN"],
        6,
    )

    # EXATAMENTE como solicitado:
    # tipo_equipamento = TIPEQUIP + descrição
    # Ex.: 07- Odontologia
    df["tipo_equipamento"] = (
        combinar_codigo_descricao(
            df["TIPEQUIP"],
            df["TIPO_EQUIPAMENTO"],
            separador="- ",
        )
    )

    # EXATAMENTE como solicitado:
    # codigo_equipamento = CODEQUIP + descrição
    # Ex.: 80 - Cadeira Odontológica Completa
    df["codigo_equipamento"] = (
        combinar_codigo_descricao(
            df["CODEQUIP"],
            df["DESC_EQUIPAMENTO"],
            separador=" - ",
        )
    )

    # ------------------------------------------------------
    # ID DE EQUIPAMENTO
    # ------------------------------------------------------
    # Mantém o mesmo esquema da Gold anterior:
    # - quando TIPEQUIP/CODEQUIP existem, usa
    #   YYYYMM + CNES + TIPEQUIP + CODEQUIP;
    # - se faltarem, usa YYYYMM + CNES + sequência;
    # - se houver repetição, acrescenta ocorrência.
    tipo = codigo(
        df["TIPEQUIP"]
    )
    cod_equip = codigo(
        df["CODEQUIP"],
        2,
    )

    sequencia = pd.Series(
        range(1, len(df) + 1),
        index=df.index,
        dtype="int64",
    ).astype(str).str.zfill(8)

    possui_tipo_codigo = (
        tipo.notna()
        & cod_equip.notna()
        & tipo.ne("")
        & cod_equip.ne("")
    )

    id_natural = (
        competencia_chave
        + cnes
        + tipo.fillna("")
        + cod_equip.fillna("")
    )

    id_artificial = (
        competencia_chave
        + cnes
        + sequencia
    )

    df["id_equipamento"] = (
        id_artificial
    )

    df.loc[
        possui_tipo_codigo,
        "id_equipamento",
    ] = id_natural[
        possui_tipo_codigo
    ]

    duplicados = (
        df["id_equipamento"]
        .duplicated(
            keep=False
        )
    )

    if duplicados.any():
        ocorrencia = (
            df.groupby(
                "id_equipamento",
                dropna=False,
            )
            .cumcount()
            .add(1)
            .astype(str)
            .str.zfill(4)
        )

        df.loc[
            duplicados,
            "id_equipamento",
        ] = (
            df.loc[
                duplicados,
                "id_equipamento",
            ].astype(str)
            + ocorrencia[
                duplicados
            ]
        )

    return reordenar_e_selecionar(
        df,
        SCHEMA_EQUIPAMENTOS.names,
    )


# ==========================================================
# FATO_LEITOS
# ==========================================================

def transformar_leitos(
    df,
    competencia,
):
    colunas = [
        "CNES",
        "CODUFMUN",
        "CODLEITO",
        "QT_CONTR",
        "QT_SUS",
        "QT_EXIST",
        "COMPETEN",
    ]

    df = garantir_colunas(
        df.copy(),
        colunas,
    )

    competencia_chave = (
        competencia_para_chave(
            df["COMPETEN"]
        )
        .fillna(competencia)
    )

    cnes = codigo(
        df["CNES"],
        7,
    )

    df["unidade_saude_id_unidade_ver"] = (
        competencia_chave
        + cnes
    )

    df["dim_data_data"] = pd.to_datetime(
        df["COMPETEN"],
        errors="coerce",
    )

    df["qt_contratada"] = pd.to_numeric(
        df["QT_CONTR"],
        errors="coerce",
    )

    df["disponivel_sus"] = pd.to_numeric(
        df["QT_SUS"],
        errors="coerce",
    )

    df["qt_existente"] = pd.to_numeric(
        df["QT_EXIST"],
        errors="coerce",
    )

    # CODLEITO já chega traduzido pela Silver.
    df["tipo_leito"] = texto(
        df["CODLEITO"]
    )

    df["dim_municipio_id_municipio"] = codigo(
        df["CODUFMUN"],
        6,
    )

    # Mesmo esquema da Gold anterior:
    # YYYYMM + CNES + sequência de 8 dígitos.
    sequencia = pd.Series(
        range(1, len(df) + 1),
        index=df.index,
        dtype="int64",
    ).astype(str).str.zfill(8)

    df["id_leitos"] = (
        competencia_chave
        + cnes
        + sequencia
    )

    return reordenar_e_selecionar(
        df,
        SCHEMA_LEITOS.names,
    )


# ==========================================================
# INTERNACAO
# ==========================================================

def transformar_internacoes(
    df,
    competencia,
):
    colunas = [
        "N_AIH",
        "DIAG_PRINC",
        "DIAG_DESCRICAO",
        "DT_INTER",
        "DT_SAIDA",
        "MUNIC_RES",
        "SEXO",
        "IDADE",
        "RACA_COR",
        "ETNIA",
        "DT_COMPETEN",
        "CNES",
        "MARCA_UTI",
        "VAL_TOT",
        "UTI_MES_TO",
        "CAR_INT",
        "MUNIC_MOV",
    ]

    df = garantir_colunas(
        df.copy(),
        colunas,
    )

    # Embora o atributo solicitado se chame
    # unidade_saude_id_unidade, o valor é a chave
    # da versão/foto da unidade: YYYYMM + CNES.
    df["unidade_saude_id_unidade"] = (
        montar_id_versao_unidade(
            df["DT_COMPETEN"],
            df["CNES"],
            competencia,
        )
    )

    df["id_internacao"] = texto(
        df["N_AIH"]
    )

    df["diagnostico"] = (
        combinar_codigo_descricao(
            df["DIAG_PRINC"],
            df["DIAG_DESCRICAO"],
            separador=" - ",
        )
    )

    df["valor_internacao"] = pd.to_numeric(
        df["VAL_TOT"],
        errors="coerce",
    )

    df["idade"] = pd.to_numeric(
        df["IDADE"],
        errors="coerce",
    )

    df["sexo"] = texto(
        df["SEXO"]
    )

    df["dias_uti"] = pd.to_numeric(
        df["UTI_MES_TO"],
        errors="coerce",
    )

    df["carater_internacao"] = texto(
        df["CAR_INT"]
    )

    df["raca_cor"] = texto(
        df["RACA_COR"]
    )

    etnia = texto(
        df["ETNIA"]
    )

    etnia = etnia.mask(
        etnia.eq("0000"),
        "Não Indígena",
    )

    etnia = etnia.fillna(
        "Não Declarada"
    )

    df["etnia"] = etnia

    df["tipo_uti"] = texto(
        df["MARCA_UTI"]
    )

    df["dim_data_data_competencia"] = pd.to_datetime(
        df["DT_COMPETEN"],
        errors="coerce",
    )

    df["dim_data_data_entrada"] = pd.to_datetime(
        df["DT_INTER"],
        errors="coerce",
    )

    df["dim_data_data_saida"] = pd.to_datetime(
        df["DT_SAIDA"],
        errors="coerce",
    )

    df["dim_municipio_municipio_res"] = codigo(
        df["MUNIC_RES"],
        6,
    )

    df["dim_municipio_municipio_int"] = codigo(
        df["MUNIC_MOV"],
        6,
    )

    return reordenar_e_selecionar(
        df,
        SCHEMA_INTERNACAO.names,
    )


# ==========================================================
# PROFISSIONAL
# ==========================================================

def transformar_profissional(
    df,
    competencia,
):
    colunas = [
        "id_profissional",
        "cns",
        "CNS",
        "COMPETEN",
        "HORAHOSP",
        "PROF_SUS",
        "CBO",
        "REGISTRO",
        "CNES",
        "CODUFMUN",
    ]

    df = garantir_colunas(
        df.copy(),
        colunas,
    )

    if df["id_profissional"].isna().any():
        raise ValueError(
            f"PF {competencia}: existem registros "
            "sem id_profissional na Silver."
        )

    if df["id_profissional"].duplicated().any():
        raise ValueError(
            f"PF {competencia}: "
            "id_profissional duplicado na Silver."
        )

    df["unidade_saude_id_unidade_ver"] = (
        montar_id_versao_unidade(
            df["COMPETEN"],
            df["CNES"],
            competencia,
        )
    )

    df["dim_data_data"] = pd.to_datetime(
        df["COMPETEN"],
        errors="coerce",
    )

    df["horas_trabalhadas"] = pd.to_numeric(
        df["HORAHOSP"],
        errors="coerce",
    )

    df["vinculo_sus"] = texto(
        df["PROF_SUS"]
    )

    # A Silver atual já entrega:
    # XXXXXX - Cargo
    # Portanto a Gold NÃO tenta zfill nem retraduzir.
    df["cbo"] = texto(
        df["CBO"]
    )

    df["registro"] = texto(
        df["REGISTRO"]
    )

    df["dim_municipio_id_municipio"] = codigo(
        df["CODUFMUN"],
        6,
    )

    # Silver atual usa "cns"; fallback para "CNS"
    # caso seja lido algum arquivo legado.
    cns_atual = texto(
        df["cns"]
    )

    cns_legado = texto(
        df["CNS"]
    )

    df["cns"] = (
        cns_atual
        .fillna(cns_legado)
    )

    return reordenar_e_selecionar(
        df,
        SCHEMA_PROFISSIONAL.names,
    )


# ==========================================================
# ALVARA
# ==========================================================

def processar_alvara(
    client,
    namespace,
):
    print(
        "--- [PROCESSANDO: ALVARA] ---"
    )

    df = ler_parquet_oci(
        client,
        namespace,
        BUCKET_SILVER,
        "dim_alvara.parquet",
    )

    df = garantir_colunas(
        df,
        [
            "id_alvara",
            "alvara",
            "data_emissao",
            "CNES_UNIDADE_VER",
        ],
    )

    if df["id_alvara"].isna().any():
        raise ValueError(
            "dim_alvara.parquet contém id_alvara nulo."
        )

    if df["id_alvara"].duplicated().any():
        raise ValueError(
            "dim_alvara.parquet contém "
            "id_alvara duplicado."
        )

    # Nome físico alinhado ao schema atual do Oracle.
    df.rename(
        columns={
            "CNES_UNIDADE_VER":
                "unidade_saude_id_unidade_ver",
        },
        inplace=True,
    )

    df = reordenar_e_selecionar(
        df,
        SCHEMA_ALVARA.names,
    )

    salvar_parquet_oci(
        client,
        namespace,
        BUCKET_GOLD,
        "ALVARA.parquet",
        df,
        schema=SCHEMA_ALVARA,
    )


# ==========================================================
# DIM_MUNICIPIO - PASSAR ADIANTE
# ==========================================================

def processar_dim_municipio(
    client,
    namespace,
):
    print(
        "--- [PROCESSANDO: DIM_MUNICIPIO] ---"
    )

    df = ler_parquet_oci(
        client,
        namespace,
        BUCKET_SILVER,
        "dim_municipio.parquet",
    )

    df = reordenar_e_selecionar(
        df,
        SCHEMA_MUNICIPIO.names,
    )

    salvar_parquet_oci(
        client,
        namespace,
        BUCKET_GOLD,
        "DIM_MUNICIPIO.parquet",
        df,
        schema=SCHEMA_MUNICIPIO,
    )


# ==========================================================
# DIM_DATA - PASSAR ADIANTE
# ==========================================================

def processar_dim_data(
    client,
    namespace,
):
    print(
        "--- [PROCESSANDO: DIM_DATA] ---"
    )

    df = ler_parquet_oci(
        client,
        namespace,
        BUCKET_SILVER,
        "dim_data.parquet",
    )

    df = reordenar_e_selecionar(
        df,
        SCHEMA_DATA.names,
    )

    salvar_parquet_oci(
        client,
        namespace,
        BUCKET_GOLD,
        "DIM_DATA.parquet",
        df,
        schema=SCHEMA_DATA,
    )


# ==========================================================
# EXECUÇÃO
# ==========================================================

def executar_transformacoes_gold():
    client, namespace = obter_cliente_oci()

    erros = []

    # ------------------------------------------------------
    # 1. UNIDADE_SAUDE
    # ------------------------------------------------------
    print(
        "\n--- [PROCESSANDO: UNIDADE_SAUDE] ---"
    )

    try:
        construir_gold_unificado(
            client=client,
            namespace=namespace,
            prefixo_silver="silver_cadastro_st_",
            object_name_gold="UNIDADE_SAUDE.parquet",
            transformar=transformar_unidade,
            schema_gold=SCHEMA_UNIDADE,
        )

    except Exception as e:
        mensagem = (
            f"❌ UNIDADE_SAUDE: {e}"
        )
        print(mensagem)
        erros.append(mensagem)

    # ------------------------------------------------------
    # 2. FATO_EQUIPAMENTOS
    # ------------------------------------------------------
    print(
        "--- [PROCESSANDO: FATO_EQUIPAMENTOS] ---"
    )

    try:
        construir_gold_unificado(
            client=client,
            namespace=namespace,
            prefixo_silver="silver_equipamentos_eq_",
            object_name_gold="FATO_EQUIPAMENTOS.parquet",
            transformar=transformar_equipamentos,
            schema_gold=SCHEMA_EQUIPAMENTOS,
        )

    except Exception as e:
        mensagem = (
            f"❌ FATO_EQUIPAMENTOS: {e}"
        )
        print(mensagem)
        erros.append(mensagem)

    # ------------------------------------------------------
    # 3. FATO_LEITOS
    # ------------------------------------------------------
    print(
        "--- [PROCESSANDO: FATO_LEITOS] ---"
    )

    try:
        construir_gold_unificado(
            client=client,
            namespace=namespace,
            prefixo_silver="silver_leitos_lt_",
            object_name_gold="FATO_LEITOS.parquet",
            transformar=transformar_leitos,
            schema_gold=SCHEMA_LEITOS,
        )

    except Exception as e:
        mensagem = (
            f"❌ FATO_LEITOS: {e}"
        )
        print(mensagem)
        erros.append(mensagem)

    # ------------------------------------------------------
    # 4. INTERNACAO
    # ------------------------------------------------------
    print(
        "--- [PROCESSANDO: INTERNACAO] ---"
    )

    try:
        construir_gold_unificado(
            client=client,
            namespace=namespace,
            prefixo_silver="silver_internacoes_rd_",
            object_name_gold="INTERNACAO.parquet",
            transformar=transformar_internacoes,
            schema_gold=SCHEMA_INTERNACAO,
        )

    except Exception as e:
        mensagem = (
            f"❌ INTERNACAO: {e}"
        )
        print(mensagem)
        erros.append(mensagem)

    # ------------------------------------------------------
    # 5. PROFISSIONAL
    # ------------------------------------------------------
    print(
        "--- [PROCESSANDO: PROFISSIONAL] ---"
    )

    try:
        construir_gold_unificado(
            client=client,
            namespace=namespace,
            prefixo_silver="silver_profissionais_pf_",
            object_name_gold="PROFISSIONAL.parquet",
            transformar=transformar_profissional,
            schema_gold=SCHEMA_PROFISSIONAL,
            usar_pyarrow=True,
            colunas_leitura=[
                "id_profissional",
                "cns",
                "CNS",
                "COMPETEN",
                "HORAHOSP",
                "PROF_SUS",
                "CBO",
                "REGISTRO",
                "CNES",
                "CODUFMUN",
            ],
        )

    except Exception as e:
        mensagem = (
            f"❌ PROFISSIONAL: {e}"
        )
        print(mensagem)
        erros.append(mensagem)

    # ------------------------------------------------------
    # 6. ALVARA
    # ------------------------------------------------------
    try:
        processar_alvara(
            client,
            namespace,
        )

    except Exception as e:
        mensagem = (
            f"❌ ALVARA: {e}"
        )
        print(mensagem)
        erros.append(mensagem)

    # ------------------------------------------------------
    # 7. DIM_MUNICIPIO
    # ------------------------------------------------------
    try:
        processar_dim_municipio(
            client,
            namespace,
        )

    except Exception as e:
        mensagem = (
            f"❌ DIM_MUNICIPIO: {e}"
        )
        print(mensagem)
        erros.append(mensagem)

    # ------------------------------------------------------
    # 8. DIM_DATA
    # ------------------------------------------------------
    try:
        processar_dim_data(
            client,
            namespace,
        )

    except Exception as e:
        mensagem = (
            f"❌ DIM_DATA: {e}"
        )
        print(mensagem)
        erros.append(mensagem)

    # ------------------------------------------------------
    # FINAL
    # ------------------------------------------------------
    print(
        "\n"
        + "=" * 70
    )

    if erros:
        print(
            "❌ GOLD FINALIZADA COM ERROS"
        )
        print(
            "=" * 70
        )

        raise RuntimeError(
            "Falhas na Gold:\n- "
            + "\n- ".join(erros)
        )

    print(
        "✅ GOLD FINALIZADA COM SUCESSO"
    )
    print(
        f"✓ 8 arquivos únicos gravados em {BUCKET_GOLD}:"
    )
    print(
        "  DIM_DATA.parquet"
    )
    print(
        "  DIM_MUNICIPIO.parquet"
    )
    print(
        "  FATO_EQUIPAMENTOS.parquet"
    )
    print(
        "  FATO_LEITOS.parquet"
    )
    print(
        "  ALVARA.parquet"
    )
    print(
        "  INTERNACAO.parquet"
    )
    print(
        "  UNIDADE_SAUDE.parquet"
    )
    print(
        "  PROFISSIONAL.parquet"
    )
    print(
        "=" * 70
    )


if __name__ == "__main__":
    executar_transformacoes_gold()
