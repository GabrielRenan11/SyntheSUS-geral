from datetime import date
from threading import Lock
from time import monotonic

from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

from database import conectar_oracle


app = FastAPI(
    title="SyntheSUS API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Comprime respostas maiores que 1 KB.
# O navegador descomprime automaticamente.
app.add_middleware(
    GZipMiddleware,
    minimum_size=1000
)


# ============================================================
# NOMES FÍSICOS / RELACIONAMENTOS IMPORTANTES DO ORACLE
# ============================================================
#
# DIM_MUNICIPIO
#   PK: id_municipio
#
# UNIDADE_SAUDE
#   PK/versionamento: id_versao_unidade
#   CNES:              id_unidade_saude
#   FK data:           dim_data_data
#   FK município:      dim_municipio_id_municipio
#
# INTERNACAO
#   FK competência:    dim_data_data_competencia
#   FK entrada:        dim_data_data_entrada
#   FK saída:          dim_data_data_saida
#   FK município res.: dim_municipio_municipio_res
#   FK município int.: dim_municipio_municipio_int
#   FK unidade/versionamento:
#                      unidade_saude_id_unidade
#
# FATO_EQUIPAMENTOS
#   FK data:           dim_data_data
#   FK município:      dim_municipio_id_municipio
#   FK unidade:        unidade_saude_id_unidade_ver
#
# FATO_LEITOS
#   FK data:           dim_data_data
#   FK município:      dim_municipio_id_municipio
#   FK unidade:        unidade_saude_id_unidade_ver
#
# ALVARA
#   data emissão:      data_emissao
#   FK unidade:        unidade_saude_id_unidade_ver
#
# PROFISSIONAL
#   FK data:           dim_data_data
#   FK município:      dim_municipio_id_municipio
#   FK unidade:        unidade_saude_id_unidade_ver
#
# REGRA:
#   id_versao_unidade é a chave usada para relacionar os registros
#   versionados à UNIDADE_SAUDE.
#   id_unidade_saude é o CNES e serve para identificar/agrupar a unidade.
# ============================================================


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def executar_consulta(sql: str, parametros: Optional[dict] = None):
    conexao = conectar_oracle()
    cursor = conexao.cursor()

    try:
        cursor.execute(sql, parametros or {})

        colunas = [col[0].lower() for col in cursor.description]

        return [
            dict(zip(colunas, linha))
            for linha in cursor.fetchall()
        ]

    finally:
        cursor.close()
        conexao.close()


def executar_consulta_unica(sql: str, parametros: Optional[dict] = None):
    dados = executar_consulta(sql, parametros)
    return dados[0] if dados else {}


def validar_periodo(data_entrada: date, data_saida: date):
    if data_saida < data_entrada:
        raise HTTPException(
            status_code=400,
            detail="data_saida não pode ser anterior a data_entrada."
        )


def adicionar_filtros_municipio(
    sql: str,
    parametros: dict,
    alias_municipio: str = "dm",
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    if regiao:
        sql += f" AND {alias_municipio}.regiao = :regiao"
        parametros["regiao"] = regiao

    if uf:
        sql += f" AND {alias_municipio}.uf = :uf"
        parametros["uf"] = uf

    if municipio:
        sql += f" AND {alias_municipio}.id_municipio = :municipio"
        parametros["municipio"] = municipio

    return sql, parametros



def competencia_para_data(competencia: str) -> date:
    """
    Recebe competência no formato YYYY-MM e devolve o primeiro dia do mês.
    """
    try:
        return date.fromisoformat(f"{competencia}-01")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Competência inválida. Use o formato YYYY-MM."
        )


def validar_periodo_competencia(
    competencia_inicio: str,
    competencia_fim: str
):
    inicio = competencia_para_data(competencia_inicio)
    fim = competencia_para_data(competencia_fim)

    if fim < inicio:
        raise HTTPException(
            status_code=400,
            detail="competencia_fim não pode ser anterior a competencia_inicio."
        )

    return inicio, fim


def adicionar_filtro_cnes(
    sql: str,
    parametros: dict,
    cnes: Optional[str] = None,
    alias_unidade: str = "u"
):
    if cnes:
        sql += f" AND {alias_unidade}.id_unidade_saude = :cnes"
        parametros["cnes"] = cnes

    return sql, parametros


def adicionar_joins_internacao_opcionais(
    sql: str,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    """
    Adiciona JOINs somente quando os filtros realmente precisam deles.

    - DIM_MUNICIPIO só é usado para região/UF/município.
    - UNIDADE_SAUDE só é usada para filtrar por CNES.

    Isso evita JOINs desnecessários nas consultas nacionais e nos
    recortes que usam apenas filtros diretamente da INTERNACAO.
    """

    if regiao or uf or municipio:
        sql += """
        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               i.dim_municipio_municipio_int
        """

    if cnes:
        sql += """
        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               i.unidade_saude_id_unidade
        """

    return sql


def condicao_sim(campo: str) -> str:
    """
    Aceita os formatos mais comuns usados na Silver/Gold para flags SUS.
    """
    return (
        f"UPPER(TRIM(TO_CHAR({campo}))) "
        "IN ('S', 'SIM', '1', 'Y', 'YES', 'TRUE')"
    )


# ============================================================
# CACHE EM MEMÓRIA PARA FILTROS GEOGRÁFICOS
# ============================================================

CACHE_FILTROS_TTL_SEGUNDOS = 3600

_cache_filtros = {}
_cache_filtros_lock = Lock()


def obter_cache_filtro(chave):
    """
    Retorna o valor armazenado se ainda estiver dentro do TTL.
    O cache é local ao processo da API.
    """
    agora = monotonic()

    with _cache_filtros_lock:
        item = _cache_filtros.get(chave)

        if item is None:
            return None

        expira_em, dados = item

        if agora >= expira_em:
            _cache_filtros.pop(chave, None)
            return None

        return dados


def salvar_cache_filtro(chave, dados):
    """
    Armazena o resultado por 1 hora.
    """
    expira_em = monotonic() + CACHE_FILTROS_TTL_SEGUNDOS

    with _cache_filtros_lock:
        _cache_filtros[chave] = (expira_em, dados)

    return dados


# ============================================================
# HOME / HEALTH
# ============================================================

@app.get("/")
def home():
    return {
        "nome": "SyntheSUS API",
        "status": "online"
    }


@app.get("/health/database")
def testar_banco():
    try:
        conexao = conectar_oracle()
        cursor = conexao.cursor()

        cursor.execute("SELECT 1 FROM DUAL")
        resultado = cursor.fetchone()[0]

        cursor.close()
        conexao.close()

        return {
            "database": "online",
            "oracle": True,
            "resultado": resultado
        }

    except Exception as erro:
        return {
            "database": "offline",
            "oracle": False,
            "erro": str(erro)
        }


# ============================================================
# FILTROS
# ============================================================

@app.get("/regioes")
def listar_regioes():
    chave_cache = ("regioes",)

    dados_cache = obter_cache_filtro(chave_cache)
    if dados_cache is not None:
        return dados_cache

    sql = """
        SELECT DISTINCT regiao
        FROM DIM_MUNICIPIO
        WHERE regiao IS NOT NULL
        ORDER BY regiao
    """

    dados = executar_consulta(sql)
    return salvar_cache_filtro(chave_cache, dados)


@app.get("/ufs")
def listar_ufs(
    regiao: Optional[str] = None
):
    chave_cache = ("ufs", regiao or "")

    dados_cache = obter_cache_filtro(chave_cache)
    if dados_cache is not None:
        return dados_cache

    sql = """
        SELECT DISTINCT uf
        FROM DIM_MUNICIPIO
        WHERE 1 = 1
    """

    parametros = {}

    if regiao:
        sql += " AND regiao = :regiao"
        parametros["regiao"] = regiao

    sql += " ORDER BY uf"

    dados = executar_consulta(sql, parametros)
    return salvar_cache_filtro(chave_cache, dados)


@app.get("/municipios")
def listar_municipios(
    uf: Optional[str] = None,
    regiao: Optional[str] = None
):
    chave_cache = ("municipios", uf or "", regiao or "")

    dados_cache = obter_cache_filtro(chave_cache)
    if dados_cache is not None:
        return dados_cache

    sql = """
        SELECT
            id_municipio,
            nome_municipio,
            uf,
            regiao
        FROM DIM_MUNICIPIO
        WHERE 1 = 1
    """

    parametros = {}

    if uf:
        sql += " AND uf = :uf"
        parametros["uf"] = uf

    if regiao:
        sql += " AND regiao = :regiao"
        parametros["regiao"] = regiao

    sql += " ORDER BY nome_municipio"

    dados = executar_consulta(sql, parametros)
    return salvar_cache_filtro(chave_cache, dados)


@app.get("/unidades")
def listar_unidades(
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    sql = """
        SELECT DISTINCT
            u.id_unidade_saude AS cnes,
            e.nome_fantasia,
            e.razao_social,
            dm.id_municipio,
            dm.nome_municipio,
            dm.uf,
            dm.regiao
        FROM UNIDADE_SAUDE u

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio = u.dim_municipio_id_municipio

        LEFT JOIN DIM_ESTABELECIMENTO e
            ON e.id_unidade_saude = u.id_unidade_saude

        WHERE 1 = 1
    """

    parametros = {}

    sql, parametros = adicionar_filtros_municipio(
        sql=sql,
        parametros=parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        ORDER BY
            e.nome_fantasia NULLS LAST,
            u.id_unidade_saude
    """

    return executar_consulta(sql, parametros)


# ============================================================
# VISÃO GERAL
# ============================================================

@app.get("/geral/resumo")
def geral_resumo(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    validar_periodo(data_entrada, data_saida)

    parametros_internacoes = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    # --------------------------------------------------------
    # INTERNAÇÕES + PERMANÊNCIA MÉDIA
    # --------------------------------------------------------
    # O filtro usa a DATA_ENTRADA da internação.
    # O limite superior usa "< data_saida + 1" para incluir
    # todo o último dia mesmo se o Oracle DATE possuir horário.
    # --------------------------------------------------------

    sql_internacoes = """
        SELECT
            COUNT(*) AS total_internacoes,
            ROUND(
                AVG(
                    CASE
                        WHEN i.dim_data_data_saida IS NOT NULL
                        THEN i.dim_data_data_saida - i.dim_data_data_entrada
                    END
                ),
                2
            ) AS permanencia_media

        FROM INTERNACAO i
    """

    sql_internacoes = adicionar_joins_internacao_opcionais(
        sql_internacoes,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql_internacoes += """
        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
    """

    sql_internacoes, parametros_internacoes = adicionar_filtros_municipio(
        sql=sql_internacoes,
        parametros=parametros_internacoes,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    internacoes = executar_consulta_unica(
        sql_internacoes,
        parametros_internacoes
    )

    # --------------------------------------------------------
    # UNIDADES NO PERÍODO
    # --------------------------------------------------------
    # UNIDADE_SAUDE é versionada por competência.
    # Consideramos versões cuja competência cai entre o
    # primeiro mês de data_entrada e o mês de data_saida.
    # --------------------------------------------------------

    parametros_unidades = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    sql_unidades = """
        SELECT
            COUNT(DISTINCT u.id_unidade_saude)
                AS total_unidades

        FROM UNIDADE_SAUDE u

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               u.dim_municipio_id_municipio

        WHERE u.dim_data_data >= TRUNC(:data_entrada, 'MM')
          AND u.dim_data_data < ADD_MONTHS(TRUNC(:data_saida, 'MM'), 1)
    """

    sql_unidades, parametros_unidades = adicionar_filtros_municipio(
        sql=sql_unidades,
        parametros=parametros_unidades,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    unidades = executar_consulta_unica(
        sql_unidades,
        parametros_unidades
    )

    # --------------------------------------------------------
    # LEITOS NA COMPETÊNCIA FINAL DO PERÍODO
    # --------------------------------------------------------
    # Leitos são fotografia mensal. Portanto não somamos
    # competências diferentes; o card usa o mês de data_saida.
    # --------------------------------------------------------

    parametros_leitos = {
        "competencia_final": data_saida
    }

    sql_leitos = """
        SELECT
            NVL(SUM(l.qt_existente), 0) AS total_leitos

        FROM FATO_LEITOS l

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               l.dim_municipio_id_municipio

        WHERE l.dim_data_data >= TRUNC(:competencia_final, 'MM')
          AND l.dim_data_data < ADD_MONTHS(TRUNC(:competencia_final, 'MM'), 1)
    """

    sql_leitos, parametros_leitos = adicionar_filtros_municipio(
        sql=sql_leitos,
        parametros=parametros_leitos,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    leitos = executar_consulta_unica(
        sql_leitos,
        parametros_leitos
    )

    return {
        "total_internacoes":
            internacoes.get("total_internacoes", 0),

        "total_leitos":
            leitos.get("total_leitos", 0),

        "permanencia_media":
            internacoes.get("permanencia_media"),

        "total_unidades":
            unidades.get("total_unidades", 0)
    }


@app.get("/geral/internacoes-evolucao")
def internacoes_evolucao(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    validar_periodo(data_entrada, data_saida)

    sql = """
        SELECT
            TO_CHAR(i.dim_data_data_entrada, 'YYYY-MM') AS competencia,
            COUNT(*) AS internacoes

        FROM INTERNACAO i
    """

    sql = adicionar_joins_internacao_opcionais(
        sql,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
    """

    parametros = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    sql, parametros = adicionar_filtros_municipio(
        sql=sql,
        parametros=parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        GROUP BY
            TO_CHAR(i.dim_data_data_entrada, 'YYYY-MM')

        ORDER BY
            competencia
    """

    return executar_consulta(sql, parametros)

@app.get("/geral/hospitais-demanda")
def hospitais_demanda(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    validar_periodo(data_entrada, data_saida)

    sql = """
        SELECT
            u.id_unidade_saude AS cnes,

            COALESCE(
                e.nome_fantasia,
                e.razao_social,
                u.id_unidade_saude
            ) AS unidade,

            COUNT(*) AS internacoes

        FROM INTERNACAO i
    """

    # O JOIN com município só é necessário se houver filtro geográfico.
    sql = adicionar_joins_internacao_opcionais(
        sql,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    # Estes JOINs são sempre necessários porque o resultado usa
    # CNES e o nome do estabelecimento.
    sql += """
        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               i.unidade_saude_id_unidade

        LEFT JOIN DIM_ESTABELECIMENTO e
            ON e.id_unidade_saude =
               u.id_unidade_saude

        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
    """

    parametros = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    sql, parametros = adicionar_filtros_municipio(
        sql=sql,
        parametros=parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        GROUP BY
            u.id_unidade_saude,
            e.nome_fantasia,
            e.razao_social

        ORDER BY
            internacoes DESC

        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)

@app.get("/geral/diagnosticos")
def geral_diagnosticos(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    validar_periodo(data_entrada, data_saida)

    sql = """
        SELECT
            i.diagnostico,
            COUNT(*) AS internacoes

        FROM INTERNACAO i
    """

    sql = adicionar_joins_internacao_opcionais(
        sql,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
          AND i.diagnostico IS NOT NULL
    """

    parametros = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    sql, parametros = adicionar_filtros_municipio(
        sql=sql,
        parametros=parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        GROUP BY
            i.diagnostico

        ORDER BY
            internacoes DESC

        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)

# ============================================================
# UNIDADES
# ============================================================

@app.get("/unidades/alvaras-resumo")
def unidades_alvaras_resumo(
    competencia_inicio: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    competencia_fim: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    inicio, fim = validar_periodo_competencia(
        competencia_inicio,
        competencia_fim
    )

    sql = """
        SELECT
            COUNT(DISTINCT u.id_unidade_saude) AS total_unidades,

            COUNT(
                DISTINCT CASE
                    WHEN h.alvara IS NOT NULL
                    THEN u.id_unidade_saude
                END
            ) AS com_alvara,

            COUNT(DISTINCT u.id_unidade_saude)
            -
            COUNT(
                DISTINCT CASE
                    WHEN h.alvara IS NOT NULL
                    THEN u.id_unidade_saude
                END
            ) AS sem_alvara

        FROM UNIDADE_SAUDE u

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               u.dim_municipio_id_municipio

        LEFT JOIN ALVARA h
            ON h.unidade_saude_id_unidade_ver =
               u.id_versao_unidade

        WHERE u.dim_data_data >= TRUNC(:competencia_inicio, 'MM')
          AND u.dim_data_data < ADD_MONTHS(TRUNC(:competencia_fim, 'MM'), 1)
    """

    parametros = {
        "competencia_inicio": inicio,
        "competencia_fim": fim
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    return executar_consulta_unica(sql, parametros)


@app.get("/unidades/tipos-ranking")
def unidades_tipos_ranking(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    competencia_data = competencia_para_data(competencia)

    sql = """
        SELECT
            u.tipo_unidade,
            COUNT(DISTINCT u.id_unidade_saude) AS unidades

        FROM UNIDADE_SAUDE u

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               u.dim_municipio_id_municipio

        WHERE u.dim_data_data >= TRUNC(:competencia, 'MM')
          AND u.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
          AND u.tipo_unidade IS NOT NULL
    """

    parametros = {
        "competencia": competencia_data
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        GROUP BY u.tipo_unidade
        ORDER BY unidades DESC
        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)


@app.get("/unidades/internacoes-evolucao")
def unidades_internacoes_evolucao(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    validar_periodo(data_entrada, data_saida)

    sql = """
        WITH internacoes_por_unidade AS (
            SELECT
                TO_CHAR(
                    i.dim_data_data_entrada,
                    'YYYY-MM'
                ) AS competencia,

                u.id_unidade_saude AS cnes,

                COUNT(*) AS qt_internacoes

            FROM INTERNACAO i

            JOIN DIM_MUNICIPIO dm
                ON dm.id_municipio =
                   i.dim_municipio_municipio_int

            JOIN UNIDADE_SAUDE u
                ON u.id_versao_unidade =
                   i.unidade_saude_id_unidade

            WHERE i.dim_data_data_entrada >= :data_entrada
              AND i.dim_data_data_entrada < :data_saida + 1
    """

    parametros = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql, parametros = adicionar_filtro_cnes(
        sql,
        parametros,
        cnes=cnes
    )

    sql += """
            GROUP BY
                TO_CHAR(i.dim_data_data_entrada, 'YYYY-MM'),
                u.id_unidade_saude
        )

        SELECT
            competencia,
            SUM(qt_internacoes) AS total_internacoes,
            ROUND(AVG(qt_internacoes), 2) AS media_por_unidade,
            ROUND(MEDIAN(qt_internacoes), 2) AS mediana_por_unidade

        FROM internacoes_por_unidade

        GROUP BY competencia
        ORDER BY competencia
    """

    return executar_consulta(sql, parametros)


@app.get("/unidades/uti-ranking")
def unidades_uti_ranking(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    validar_periodo(data_entrada, data_saida)

    sql = """
        SELECT
            u.id_unidade_saude AS cnes,
            ROUND(AVG(i.dias_uti), 2) AS permanencia_media_uti,
            COUNT(*) AS internacoes

        FROM INTERNACAO i

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               i.dim_municipio_municipio_int

        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               i.unidade_saude_id_unidade

        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
          AND i.dias_uti IS NOT NULL
    """

    parametros = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        GROUP BY u.id_unidade_saude
        ORDER BY permanencia_media_uti DESC
        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)


# Rotas dinâmicas de /unidades vêm DEPOIS das rotas estáticas acima.

@app.get("/unidades/{cnes}/alvara")
def unidade_alvara(cnes: str):
    sql = """
        SELECT
            u.id_unidade_saude AS cnes,
            h.alvara,
            h.data_emissao AS data_emissao

        FROM UNIDADE_SAUDE u

        JOIN ALVARA h
            ON h.unidade_saude_id_unidade_ver =
               u.id_versao_unidade

        WHERE u.id_unidade_saude = :cnes
          AND h.alvara IS NOT NULL

        ORDER BY
            h.data_emissao DESC NULLS LAST

        FETCH FIRST 1 ROW ONLY
    """

    return executar_consulta_unica(
        sql,
        {"cnes": cnes}
    )


@app.get("/unidades/{cnes}/infraestrutura")
def unidade_infraestrutura(
    cnes: str,
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$")
):
    competencia_data = competencia_para_data(competencia)

    # Uma única chamada ao Oracle retorna leitos, equipamentos,
    # profissionais e o total de profissionais da unidade.
    sql = """
        WITH leitos AS (
            SELECT
                l.tipo_leito AS categoria,
                NVL(SUM(l.qt_existente), 0) AS quantidade

            FROM FATO_LEITOS l

            JOIN UNIDADE_SAUDE u
                ON u.id_versao_unidade =
                   l.unidade_saude_id_unidade_ver

            WHERE u.id_unidade_saude = :cnes
              AND l.dim_data_data >= TRUNC(:competencia, 'MM')
              AND l.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
              AND l.tipo_leito IS NOT NULL

            GROUP BY l.tipo_leito
        ),

        equipamentos AS (
            SELECT
                e.tipo_equipamento AS categoria,
                NVL(SUM(e.qt_equipamentos), 0) AS quantidade

            FROM FATO_EQUIPAMENTOS e

            JOIN UNIDADE_SAUDE u
                ON u.id_versao_unidade =
                   e.unidade_saude_id_unidade_ver

            WHERE u.id_unidade_saude = :cnes
              AND e.dim_data_data >= TRUNC(:competencia, 'MM')
              AND e.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
              AND e.tipo_equipamento IS NOT NULL

            GROUP BY e.tipo_equipamento
        ),

        profissionais AS (
            SELECT
                p.cbo AS categoria,
                COUNT(DISTINCT p.cns) AS quantidade,
                GROUPING(p.cbo) AS total_flag

            FROM PROFISSIONAL p

            JOIN UNIDADE_SAUDE u
                ON u.id_versao_unidade =
                   p.unidade_saude_id_unidade_ver

            WHERE u.id_unidade_saude = :cnes
              AND p.dim_data_data >= TRUNC(:competencia, 'MM')
              AND p.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
              AND p.cns IS NOT NULL

            GROUP BY GROUPING SETS (
                (p.cbo),
                ()
            )
        )

        SELECT
            'leitos' AS grupo,
            categoria,
            quantidade
        FROM leitos

        UNION ALL

        SELECT
            'equipamentos' AS grupo,
            categoria,
            quantidade
        FROM equipamentos

        UNION ALL

        SELECT
            CASE
                WHEN total_flag = 1 THEN 'total_profissionais'
                ELSE 'profissionais'
            END AS grupo,
            categoria,
            quantidade
        FROM profissionais
        WHERE total_flag = 1
           OR categoria IS NOT NULL

        ORDER BY grupo, quantidade DESC
    """

    parametros = {
        "cnes": cnes,
        "competencia": competencia_data
    }

    linhas = executar_consulta(sql, parametros)

    leitos = []
    equipamentos = []
    profissionais = []
    total_profissionais = 0

    for linha in linhas:
        grupo = linha.get("grupo")
        categoria = linha.get("categoria")
        quantidade = linha.get("quantidade", 0)

        if grupo == "leitos":
            leitos.append({
                "tipo_leito": categoria,
                "quantidade": quantidade
            })

        elif grupo == "equipamentos":
            equipamentos.append({
                "tipo_equipamento": categoria,
                "quantidade": quantidade
            })

        elif grupo == "profissionais":
            profissionais.append({
                "cbo": categoria,
                "quantidade": quantidade
            })

        elif grupo == "total_profissionais":
            total_profissionais = quantidade

    return {
        "cnes": cnes,
        "competencia": competencia,
        "leitos": leitos,
        "equipamentos": equipamentos,
        "profissionais": profissionais,
        "total_profissionais": total_profissionais
    }


# ============================================================
# DISPONIBILIDADE DE LEITOS
# ============================================================

@app.get("/leitos/resumo")
def leitos_resumo(
    competencia_inicio: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    competencia_fim: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    _, fim = validar_periodo_competencia(
        competencia_inicio,
        competencia_fim
    )

    # Os cards representam a fotografia da competência FINAL.
    sql = """
        SELECT
            NVL(SUM(l.qt_existente), 0) AS total_leitos_existentes,
            NVL(SUM(l.disponivel_sus), 0) AS leitos_disponiveis_sus,
            NVL(SUM(l.qt_contratada), 0) AS leitos_contratados,
            COUNT(
                DISTINCT CASE
                    WHEN NVL(l.qt_existente, 0) > 0
                    THEN u.id_unidade_saude
                END
            ) AS unidades_com_leitos

        FROM FATO_LEITOS l

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               l.dim_municipio_id_municipio

        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               l.unidade_saude_id_unidade_ver

        WHERE l.dim_data_data >= TRUNC(:competencia_fim, 'MM')
          AND l.dim_data_data < ADD_MONTHS(TRUNC(:competencia_fim, 'MM'), 1)
    """

    parametros = {
        "competencia_fim": fim
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql, parametros = adicionar_filtro_cnes(
        sql,
        parametros,
        cnes=cnes
    )

    return executar_consulta_unica(sql, parametros)


@app.get("/leitos/unidades")
def leitos_unidades(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    competencia_data = competencia_para_data(competencia)

    sql = """
        SELECT
            u.id_unidade_saude AS cnes,
            dm.nome_municipio,
            dm.uf,
            SUM(l.qt_existente) AS leitos_existentes

        FROM FATO_LEITOS l

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               l.dim_municipio_id_municipio

        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               l.unidade_saude_id_unidade_ver

        WHERE l.dim_data_data >= TRUNC(:competencia, 'MM')
          AND l.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
    """

    parametros = {
        "competencia": competencia_data
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        GROUP BY
            u.id_unidade_saude,
            dm.nome_municipio,
            dm.uf

        HAVING SUM(l.qt_existente) > 0

        ORDER BY leitos_existentes DESC
    """

    return executar_consulta(sql, parametros)


@app.get("/leitos/ranking-unidades")
def leitos_ranking_unidades(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None
):
    competencia_data = competencia_para_data(competencia)

    sql = """
        SELECT
            u.id_unidade_saude AS cnes,
            SUM(l.qt_existente) AS leitos_existentes

        FROM FATO_LEITOS l

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               l.dim_municipio_id_municipio

        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               l.unidade_saude_id_unidade_ver

        WHERE l.dim_data_data >= TRUNC(:competencia, 'MM')
          AND l.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
    """

    parametros = {
        "competencia": competencia_data
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql += """
        GROUP BY u.id_unidade_saude
        ORDER BY leitos_existentes DESC
        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)


@app.get("/leitos/ranking-tipos")
def leitos_ranking_tipos(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    competencia_data = competencia_para_data(competencia)

    sql = """
        SELECT
            l.tipo_leito,
            SUM(l.qt_existente) AS leitos_existentes

        FROM FATO_LEITOS l

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               l.dim_municipio_id_municipio

        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               l.unidade_saude_id_unidade_ver

        WHERE l.dim_data_data >= TRUNC(:competencia, 'MM')
          AND l.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
          AND l.tipo_leito IS NOT NULL
    """

    parametros = {
        "competencia": competencia_data
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql, parametros = adicionar_filtro_cnes(
        sql,
        parametros,
        cnes=cnes
    )

    sql += """
        GROUP BY l.tipo_leito
        ORDER BY leitos_existentes DESC
        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)


# ============================================================
# DISPONIBILIDADE DE EQUIPAMENTOS
# ============================================================

@app.get("/equipamentos/resumo")
def equipamentos_resumo(
    competencia_inicio: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    competencia_fim: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None,
    tipo: Optional[str] = None,
    subtipo: Optional[str] = None
):
    _, fim = validar_periodo_competencia(
        competencia_inicio,
        competencia_fim
    )

    sql = f"""
        SELECT
            NVL(SUM(e.qt_equipamentos), 0)
                AS total_equipamentos_cadastrados,

            NVL(SUM(e.qt_uso), 0)
                AS total_equipamentos_operacionais,

            NVL(
                SUM(
                    CASE
                        WHEN {condicao_sim("e.disponibilidade_sus")}
                        THEN e.qt_equipamentos
                        ELSE 0
                    END
                ),
                0
            ) AS total_equipamentos_sus

        FROM FATO_EQUIPAMENTOS e

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               e.dim_municipio_id_municipio

        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               e.unidade_saude_id_unidade_ver

        WHERE e.dim_data_data >= TRUNC(:competencia_fim, 'MM')
          AND e.dim_data_data < ADD_MONTHS(TRUNC(:competencia_fim, 'MM'), 1)
    """

    parametros = {
        "competencia_fim": fim
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql, parametros = adicionar_filtro_cnes(
        sql,
        parametros,
        cnes=cnes
    )

    if tipo:
        sql += " AND e.tipo_equipamento = :tipo"
        parametros["tipo"] = tipo

    if subtipo:
        sql += " AND e.codigo_equipamento = :subtipo"
        parametros["subtipo"] = subtipo

    return executar_consulta_unica(sql, parametros)


@app.get("/equipamentos/tipos")
def equipamentos_tipos(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    competencia_data = competencia_para_data(competencia)

    sql = """
        SELECT DISTINCT
            e.tipo_equipamento
        FROM FATO_EQUIPAMENTOS e
        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio = e.dim_municipio_id_municipio
        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade = e.unidade_saude_id_unidade_ver
        WHERE e.dim_data_data >= TRUNC(:competencia, 'MM')
          AND e.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
          AND e.tipo_equipamento IS NOT NULL
    """
    parametros = {"competencia": competencia_data}

    sql, parametros = adicionar_filtros_municipio(
        sql, parametros, regiao=regiao, uf=uf, municipio=municipio
    )
    sql, parametros = adicionar_filtro_cnes(sql, parametros, cnes=cnes)

    sql += " ORDER BY e.tipo_equipamento"
    return executar_consulta(sql, parametros)


@app.get("/equipamentos/subtipos")
def equipamentos_subtipos(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    tipo: str = Query(...),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    competencia_data = competencia_para_data(competencia)

    sql = """
        SELECT DISTINCT
            e.codigo_equipamento
        FROM FATO_EQUIPAMENTOS e
        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio = e.dim_municipio_id_municipio
        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade = e.unidade_saude_id_unidade_ver
        WHERE e.dim_data_data >= TRUNC(:competencia, 'MM')
          AND e.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
          AND e.tipo_equipamento = :tipo
          AND e.codigo_equipamento IS NOT NULL
    """
    parametros = {
        "competencia": competencia_data,
        "tipo": tipo
    }

    sql, parametros = adicionar_filtros_municipio(
        sql, parametros, regiao=regiao, uf=uf, municipio=municipio
    )
    sql, parametros = adicionar_filtro_cnes(sql, parametros, cnes=cnes)

    sql += " ORDER BY e.codigo_equipamento"
    return executar_consulta(sql, parametros)


@app.get("/equipamentos/ranking-tipos")
def equipamentos_ranking_tipos(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None,
    tipo: Optional[str] = None
):
    competencia_data = competencia_para_data(competencia)

    campo = "e.codigo_equipamento" if tipo else "e.tipo_equipamento"

    sql = f"""
        SELECT
            {campo} AS nome,
            SUM(e.qt_equipamentos) AS quantidade
        FROM FATO_EQUIPAMENTOS e
        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio = e.dim_municipio_id_municipio
        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade = e.unidade_saude_id_unidade_ver
        WHERE e.dim_data_data >= TRUNC(:competencia, 'MM')
          AND e.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
          AND {campo} IS NOT NULL
    """
    parametros = {"competencia": competencia_data}

    sql, parametros = adicionar_filtros_municipio(
        sql, parametros, regiao=regiao, uf=uf, municipio=municipio
    )
    sql, parametros = adicionar_filtro_cnes(sql, parametros, cnes=cnes)

    if tipo:
        sql += " AND e.tipo_equipamento = :tipo"
        parametros["tipo"] = tipo

    sql += f"""
        GROUP BY {campo}
        ORDER BY quantidade DESC
        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)


@app.get("/equipamentos/evolucao")
def equipamentos_evolucao(
    competencia_inicio: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    competencia_fim: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None,
    tipo: Optional[str] = None,
    subtipo: Optional[str] = None
):
    inicio, fim = validar_periodo_competencia(
        competencia_inicio,
        competencia_fim
    )

    sql = f"""
        WITH equipamentos_por_unidade AS (
            SELECT
                TO_CHAR(e.dim_data_data, 'YYYY-MM')
                    AS competencia,

                u.id_unidade_saude AS cnes,

                SUM(e.qt_equipamentos)
                    AS cadastrados,

                SUM(e.qt_uso)
                    AS operacionais,

                SUM(
                    CASE
                        WHEN {condicao_sim("e.disponibilidade_sus")}
                        THEN e.qt_equipamentos
                        ELSE 0
                    END
                ) AS disponiveis_sus

            FROM FATO_EQUIPAMENTOS e

            JOIN DIM_MUNICIPIO dm
                ON dm.id_municipio =
                   e.dim_municipio_id_municipio

            JOIN UNIDADE_SAUDE u
                ON u.id_versao_unidade =
                   e.unidade_saude_id_unidade_ver

            WHERE e.dim_data_data >= TRUNC(:competencia_inicio, 'MM')
              AND e.dim_data_data <
                  ADD_MONTHS(TRUNC(:competencia_fim, 'MM'), 1)
    """

    parametros = {
        "competencia_inicio": inicio,
        "competencia_fim": fim
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql, parametros = adicionar_filtro_cnes(
        sql,
        parametros,
        cnes=cnes
    )

    if tipo:
        sql += " AND e.tipo_equipamento = :tipo"
        parametros["tipo"] = tipo

    if subtipo:
        sql += " AND e.codigo_equipamento = :subtipo"
        parametros["subtipo"] = subtipo

    sql += """
            GROUP BY
                TO_CHAR(e.dim_data_data, 'YYYY-MM'),
                u.id_unidade_saude
        )

        SELECT
            competencia,
            ROUND(AVG(cadastrados), 2) AS media_cadastrados,
            ROUND(AVG(operacionais), 2) AS media_operacionais,
            ROUND(AVG(disponiveis_sus), 2) AS media_disponiveis_sus

        FROM equipamentos_por_unidade

        GROUP BY competencia
        ORDER BY competencia
    """

    return executar_consulta(sql, parametros)


# ============================================================
# DISPONIBILIDADE DE PROFISSIONAIS
# ============================================================

@app.get("/profissionais/resumo")
def profissionais_resumo(
    competencia_inicio: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    competencia_fim: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    _, fim = validar_periodo_competencia(
        competencia_inicio,
        competencia_fim
    )

    # A mesma base filtrada alimenta tanto os cards quanto
    # a média de profissionais distintos por unidade.
    base_sql = f"""
        WITH base AS (
            SELECT
                u.id_unidade_saude AS cnes_unidade,
                p.cns,
                p.vinculo_sus,
                p.horas_trabalhadas

            FROM PROFISSIONAL p

            JOIN DIM_MUNICIPIO dm
                ON dm.id_municipio =
                   p.dim_municipio_id_municipio

            JOIN UNIDADE_SAUDE u
                ON u.id_versao_unidade =
                   p.unidade_saude_id_unidade_ver

            WHERE p.dim_data_data >= TRUNC(:competencia_fim, 'MM')
              AND p.dim_data_data < ADD_MONTHS(TRUNC(:competencia_fim, 'MM'), 1)
              AND p.cns IS NOT NULL
    """

    parametros = {
        "competencia_fim": fim
    }

    base_sql, parametros = adicionar_filtros_municipio(
        base_sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    base_sql, parametros = adicionar_filtro_cnes(
        base_sql,
        parametros,
        cnes=cnes
    )

    sql = base_sql + f"""
        ),

        por_unidade AS (
            SELECT
                cnes_unidade,
                COUNT(DISTINCT cns) AS qt_profissionais
            FROM base
            GROUP BY cnes_unidade
        ),

        resumo AS (
            SELECT
                COUNT(DISTINCT b.cns)
                    AS total_profissionais_cadastrados,

                COUNT(
                    DISTINCT CASE
                        WHEN {condicao_sim("b.vinculo_sus")}
                        THEN b.cns
                    END
                ) AS total_profissionais_sus,

                ROUND(AVG(b.horas_trabalhadas), 2)
                    AS carga_horaria_media

            FROM base b
        ),

        media_unidade AS (
            SELECT
                ROUND(AVG(qt_profissionais), 2)
                    AS media_profissionais_por_unidade
            FROM por_unidade
        )

        SELECT
            r.total_profissionais_cadastrados,
            r.total_profissionais_sus,
            r.carga_horaria_media,
            m.media_profissionais_por_unidade

        FROM resumo r
        CROSS JOIN media_unidade m
    """

    resumo = executar_consulta_unica(
        sql,
        parametros
    )

    resumo["carga_horaria_disponivel"] = (
        resumo.get("carga_horaria_media") is not None
    )

    return resumo


@app.get("/profissionais/ocupacoes")
def profissionais_ocupacoes(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    competencia_data = competencia_para_data(competencia)

    sql = """
        SELECT
            p.cbo,
            COUNT(DISTINCT p.cns) AS profissionais

        FROM PROFISSIONAL p

        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio =
               p.dim_municipio_id_municipio

        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade =
               p.unidade_saude_id_unidade_ver

        WHERE p.dim_data_data >= TRUNC(:competencia, 'MM')
          AND p.dim_data_data < ADD_MONTHS(TRUNC(:competencia, 'MM'), 1)
          AND p.cns IS NOT NULL
          AND p.cbo IS NOT NULL
    """

    parametros = {
        "competencia": competencia_data
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql, parametros = adicionar_filtro_cnes(
        sql,
        parametros,
        cnes=cnes
    )

    sql += """
        GROUP BY p.cbo
        ORDER BY profissionais DESC
        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)


# ============================================================
# INTERNAÇÕES
# ============================================================

def adicionar_filtros_internacao(
    sql: str,
    parametros: dict,
    cnes: Optional[str] = None,
    sexo: Optional[str] = None,
    raca_cor: Optional[str] = None,
    etnia: Optional[str] = None,
    carater_internacao: Optional[str] = None,
    idade_min: Optional[int] = None,
    idade_max: Optional[int] = None
):
    if cnes:
        sql += " AND u.id_unidade_saude = :cnes"
        parametros["cnes"] = cnes

    if sexo:
        sql += " AND i.sexo = :sexo"
        parametros["sexo"] = sexo

    if raca_cor:
        sql += " AND i.raca_cor = :raca_cor"
        parametros["raca_cor"] = raca_cor

    if etnia:
        sql += " AND i.etnia = :etnia"
        parametros["etnia"] = etnia

    if carater_internacao:
        sql += " AND i.carater_internacao = :carater_internacao"
        parametros["carater_internacao"] = carater_internacao

    if idade_min is not None:
        sql += " AND i.idade >= :idade_min"
        parametros["idade_min"] = idade_min

    if idade_max is not None:
        sql += " AND i.idade <= :idade_max"
        parametros["idade_max"] = idade_max

    return sql, parametros


@app.get("/internacoes/resumo")
def internacoes_resumo(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None,
    sexo: Optional[str] = None,
    raca_cor: Optional[str] = None,
    etnia: Optional[str] = None,
    carater_internacao: Optional[str] = None,
    idade_min: Optional[int] = None,
    idade_max: Optional[int] = None
):
    validar_periodo(data_entrada, data_saida)

    sql = """
        SELECT
            COUNT(*) AS total_internacoes,

            ROUND(
                AVG(
                    CASE
                        WHEN i.dim_data_data_saida IS NOT NULL
                        THEN i.dim_data_data_saida -
                             i.dim_data_data_entrada
                    END
                ),
                2
            ) AS permanencia_media,

            ROUND(AVG(i.dias_uti), 2)
                AS permanencia_media_uti,

            ROUND(AVG(i.valor_internacao), 2)
                AS valor_medio_internacoes

        FROM INTERNACAO i
    """

    sql = adicionar_joins_internacao_opcionais(
        sql,
        regiao=regiao,
        uf=uf,
        municipio=municipio,
        cnes=cnes
    )

    sql += """
        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
    """

    parametros = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql, parametros = adicionar_filtros_internacao(
        sql,
        parametros,
        cnes=cnes,
        sexo=sexo,
        raca_cor=raca_cor,
        etnia=etnia,
        carater_internacao=carater_internacao,
        idade_min=idade_min,
        idade_max=idade_max
    )

    return executar_consulta_unica(sql, parametros)

@app.get("/internacoes/diagnosticos")
def internacoes_diagnosticos(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None,
    sexo: Optional[str] = None,
    raca_cor: Optional[str] = None,
    etnia: Optional[str] = None,
    carater_internacao: Optional[str] = None,
    idade_min: Optional[int] = None,
    idade_max: Optional[int] = None
):
    validar_periodo(data_entrada, data_saida)

    sql = """
        SELECT
            i.diagnostico,
            COUNT(*) AS internacoes

        FROM INTERNACAO i
    """

    sql = adicionar_joins_internacao_opcionais(
        sql,
        regiao=regiao,
        uf=uf,
        municipio=municipio,
        cnes=cnes
    )

    sql += """
        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
          AND i.diagnostico IS NOT NULL
    """

    parametros = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    sql, parametros = adicionar_filtros_municipio(
        sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    sql, parametros = adicionar_filtros_internacao(
        sql,
        parametros,
        cnes=cnes,
        sexo=sexo,
        raca_cor=raca_cor,
        etnia=etnia,
        carater_internacao=carater_internacao,
        idade_min=idade_min,
        idade_max=idade_max
    )

    sql += """
        GROUP BY i.diagnostico
        ORDER BY internacoes DESC
        FETCH FIRST 10 ROWS ONLY
    """

    return executar_consulta(sql, parametros)

@app.get("/internacoes/filtros")
def internacoes_filtros(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None
):
    validar_periodo(data_entrada, data_saida)

    base_sql = """
        FROM INTERNACAO i
        JOIN DIM_MUNICIPIO dm
            ON dm.id_municipio = i.dim_municipio_municipio_int
        JOIN UNIDADE_SAUDE u
            ON u.id_versao_unidade = i.unidade_saude_id_unidade
        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
    """
    parametros = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    base_sql, parametros = adicionar_filtros_municipio(
        base_sql,
        parametros,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )
    base_sql, parametros = adicionar_filtros_internacao(
        base_sql,
        parametros,
        cnes=cnes
    )

    def distintos(coluna: str):
        sql = f"""
            SELECT DISTINCT {coluna} AS valor
            {base_sql}
              AND {coluna} IS NOT NULL
            ORDER BY valor
        """
        return executar_consulta(sql, parametros)

    return {
        "sexo": distintos("i.sexo"),
        "raca_cor": distintos("i.raca_cor"),
        "etnia": distintos("i.etnia"),
        "carater_internacao": distintos("i.carater_internacao")
    }


@app.get("/internacoes/demografia")
def internacoes_demografia(
    data_entrada: date,
    data_saida: date,
    regiao: Optional[str] = None,
    uf: Optional[str] = None,
    municipio: Optional[str] = None,
    cnes: Optional[str] = None,
    carater_internacao: Optional[str] = None,
    sexo: Optional[str] = None,
    raca_cor: Optional[str] = None,
    etnia: Optional[str] = None,
    idade_min: Optional[int] = None,
    idade_max: Optional[int] = None
):
    validar_periodo(data_entrada, data_saida)

    parametros_base = {
        "data_entrada": data_entrada,
        "data_saida": data_saida
    }

    base_sql = """
        FROM INTERNACAO i
    """

    base_sql = adicionar_joins_internacao_opcionais(
        base_sql,
        regiao=regiao,
        uf=uf,
        municipio=municipio,
        cnes=cnes
    )

    base_sql += """
        WHERE i.dim_data_data_entrada >= :data_entrada
          AND i.dim_data_data_entrada < :data_saida + 1
    """

    base_sql, parametros_base = adicionar_filtros_municipio(
        base_sql,
        parametros_base,
        regiao=regiao,
        uf=uf,
        municipio=municipio
    )

    base_sql, parametros_base = adicionar_filtros_internacao(
        base_sql,
        parametros_base,
        cnes=cnes,
        sexo=sexo,
        raca_cor=raca_cor,
        etnia=etnia,
        carater_internacao=carater_internacao,
        idade_min=idade_min,
        idade_max=idade_max
    )

    # Uma única consulta Oracle gera as quatro distribuições demográficas.
    # GROUPING SETS evita quatro round-trips separados ao banco.
    sql = """
        WITH dados_filtrados AS (
            SELECT
                i.sexo,
                i.raca_cor,
                i.etnia,
                CASE
                    WHEN i.idade BETWEEN 0 AND 17 THEN '0-17'
                    WHEN i.idade BETWEEN 18 AND 29 THEN '18-29'
                    WHEN i.idade BETWEEN 30 AND 44 THEN '30-44'
                    WHEN i.idade BETWEEN 45 AND 59 THEN '45-59'
                    WHEN i.idade BETWEEN 60 AND 74 THEN '60-74'
                    WHEN i.idade >= 75 THEN '75+'
                END AS faixa
    """ + base_sql + """
        )
        SELECT
            CASE
                WHEN GROUPING(sexo) = 0 THEN 'sexo'
                WHEN GROUPING(faixa) = 0 THEN 'idade'
                WHEN GROUPING(raca_cor) = 0 THEN 'raca_cor'
                WHEN GROUPING(etnia) = 0 THEN 'etnia'
            END AS grupo,

            CASE
                WHEN GROUPING(sexo) = 0 THEN sexo
                WHEN GROUPING(faixa) = 0 THEN faixa
                WHEN GROUPING(raca_cor) = 0 THEN raca_cor
                WHEN GROUPING(etnia) = 0 THEN etnia
            END AS categoria,

            COUNT(*) AS quantidade

        FROM dados_filtrados

        GROUP BY GROUPING SETS (
            (sexo),
            (faixa),
            (raca_cor),
            (etnia)
        )

        HAVING
               (GROUPING(sexo) = 0 AND sexo IS NOT NULL)
            OR (GROUPING(faixa) = 0 AND faixa IS NOT NULL)
            OR (GROUPING(raca_cor) = 0 AND raca_cor IS NOT NULL)
            OR (GROUPING(etnia) = 0 AND etnia IS NOT NULL)

        ORDER BY grupo, quantidade DESC
    """

    linhas = executar_consulta(sql, parametros_base)

    resultado = {
        "sexo": [],
        "idade": [],
        "raca_cor": [],
        "etnia": []
    }

    for linha in linhas:
        grupo = linha.get("grupo")
        categoria = linha.get("categoria")
        quantidade = linha.get("quantidade")

        if grupo == "idade":
            resultado["idade"].append({
                "faixa": categoria,
                "quantidade": quantidade
            })
        elif grupo in ("sexo", "raca_cor", "etnia"):
            resultado[grupo].append({
                "categoria": categoria,
                "quantidade": quantidade
            })

    # Mantém o comportamento anterior: etnia limitada ao top 10.
    resultado["etnia"] = resultado["etnia"][:10]

    return resultado