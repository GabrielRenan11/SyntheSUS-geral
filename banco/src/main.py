import argparse
import os
from datetime import datetime

from oracle_connection import get_oracle_connection

from oci_storage import (
    listar_objetos_gold,
    parquet_gold_temporario
)

from loader import (
    analisar_parquet_por_competencia,
    deletar_competencias,
    inserir_competencias_em_batches,
    merge_competencias_em_batches,
    merge_parquet_em_batches,
    obter_contagens_oracle_por_competencia,
    remover_fora_retencao,
    validar_identificador
)

from controle_carga import (
    carga_ja_realizada,
    existe_controle_competencia_tabela,
    obter_controles_competencias_sucesso,
    obter_ultima_carga_sucesso,
    registrar_carga,
    registrar_carga_competencia
)

from tables import (
    TABELAS,
    TABELAS_POR_NOME
)


RETENCAO_MESES = int(
    os.getenv(
        "RETENCAO_MESES",
        "12"
    )
)


# ============================================================
# TESTAR ORACLE
# ============================================================

def testar_oracle():
    connection = get_oracle_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            "SELECT USER, SYSDATE FROM DUAL"
        )

        usuario, data = cursor.fetchone()

        print(
            f"Oracle OK | "
            f"Usuário: {usuario} | "
            f"Data: {data}"
        )

    finally:
        cursor.close()
        connection.close()


# ============================================================
# CONTAR REGISTROS
# ============================================================

def contar_registros(
    connection,
    tabela
):
    tabela = validar_identificador(
        tabela
    )

    cursor = connection.cursor()

    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM {tabela}"
        )

        return cursor.fetchone()[0]

    finally:
        cursor.close()


# ============================================================
# COMPETÊNCIA -> DATETIME
# ============================================================

def competencia_para_datetime(
    competencia
):
    return datetime.strptime(
        competencia,
        "%Y%m"
    )


# ============================================================
# LIMITE DA JANELA MÓVEL
# ============================================================

def obter_limite_retencao():
    hoje = datetime.now()

    inicio_mes_atual = datetime(
        hoje.year,
        hoje.month,
        1
    )

    indice = (
        inicio_mes_atual.year * 12
        + inicio_mes_atual.month
        - 1
        - RETENCAO_MESES
    )

    ano, mes_zero = divmod(
        indice,
        12
    )

    return datetime(
        ano,
        mes_zero + 1,
        1
    )


# ============================================================
# BOOTSTRAP GLOBAL ANTIGO
# ============================================================

def bootstrap_controle(
    objetos_por_nome
):
    print("\n" + "=" * 70)
    print("BOOTSTRAP DO CONTROLE GLOBAL")
    print("=" * 70)

    print(
        "\nATENÇÃO: este modo NÃO carrega dados."
    )

    for config in TABELAS:
        tabela = config["tabela"]
        arquivo = config["arquivo"]

        print("\n" + "-" * 70)
        print(tabela)
        print("-" * 70)

        if arquivo not in objetos_por_nome:
            print(
                f"SKIP: {arquivo} não encontrado."
            )
            continue

        etag = objetos_por_nome[
            arquivo
        ]["etag"]

        connection = get_oracle_connection()

        try:
            if carga_ja_realizada(
                connection,
                tabela,
                arquivo,
                etag
            ):
                print(
                    "Controle global já existe. SKIP."
                )
                continue

            total_oracle = contar_registros(
                connection,
                tabela
            )

            if total_oracle == 0:
                print(
                    "Tabela vazia. "
                    "Nenhum bootstrap realizado."
                )
                continue

            registrar_carga(
                connection=connection,
                tabela=tabela,
                arquivo=arquivo,
                etag=etag,
                qt_registros=total_oracle,
                status="SUCESSO"
            )

            connection.commit()

            print(
                f"Bootstrap global OK | "
                f"ETag={etag} | "
                f"Registros={total_oracle:,}"
            )

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    print("\n" + "=" * 70)
    print("BOOTSTRAP GLOBAL FINALIZADO")
    print("=" * 70)


# ============================================================
# BOOTSTRAP POR COMPETÊNCIA
# ============================================================

def bootstrap_competencias(
    objetos_por_nome
):
    print("\n" + "=" * 70)
    print("BOOTSTRAP DO CONTROLE POR COMPETÊNCIA")
    print("=" * 70)

    print(
        "\nEste modo NÃO altera dados das tabelas."
    )

    print(
        "Ele calcula hashes da Gold atual e só registra "
        "competências cuja contagem bate com o Oracle."
    )

    for config in TABELAS:
        estrategia = config["estrategia"]

        if estrategia not in {
            "replace_competencia",
            "merge_competencia"
        }:
            continue

        tabela = config["tabela"]
        arquivo = config["arquivo"]

        print("\n" + "-" * 70)
        print(tabela)
        print("-" * 70)

        if arquivo not in objetos_por_nome:
            print(
                f"SKIP: {arquivo} não encontrado."
            )
            continue

        etag = objetos_por_nome[
            arquivo
        ]["etag"]

        connection = get_oracle_connection()

        try:
            total_oracle = contar_registros(
                connection,
                tabela
            )

            if total_oracle == 0:
                print(
                    "Tabela vazia. "
                    "Bootstrap por competência ignorado."
                )
                continue

            if not carga_ja_realizada(
                connection=connection,
                tabela=tabela,
                arquivo=arquivo,
                etag=etag
            ):
                raise RuntimeError(
                    f"{tabela}: o ETag Gold atual não está "
                    "registrado como SUCESSO em "
                    "CONTROLE_CARGA_GOLD. "
                    "Não é seguro fazer bootstrap desse arquivo."
                )

            with parquet_gold_temporario(
                arquivo
            ) as caminho:
                metadados_comp = (
                    analisar_parquet_por_competencia(
                        caminho,
                        config
                    )
                )

            contagens_oracle = (
                obter_contagens_oracle_por_competencia(
                    connection,
                    config
                )
            )

            controles_atuais = (
                obter_controles_competencias_sucesso(
                    connection,
                    tabela
                )
            )

            for competencia, meta in metadados_comp.items():
                esperado = meta[
                    "qt_registros"
                ]

                encontrado = contagens_oracle.get(
                    competencia,
                    0
                )

                if encontrado != esperado:
                    raise RuntimeError(
                        f"{tabela} {competencia}: "
                        f"Gold={esperado:,} | "
                        f"Oracle={encontrado:,}. "
                        "Bootstrap interrompido."
                    )

                controle = controles_atuais.get(
                    competencia
                )

                if (
                    controle is not None
                    and controle["hash"] == meta["hash"]
                    and controle["qt_registros"] == esperado
                ):
                    print(
                        f"{competencia}: controle já existe. SKIP."
                    )
                    continue

                registrar_carga_competencia(
                    connection=connection,
                    tabela=tabela,
                    competencia=competencia_para_datetime(
                        competencia
                    ),
                    hash_competencia=meta["hash"],
                    qt_registros=esperado,
                    etag_arquivo=etag,
                    status="SUCESSO"
                )

                print(
                    f"{competencia}: bootstrap OK | "
                    f"{esperado:,} registros"
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    print("\n" + "=" * 70)
    print("BOOTSTRAP POR COMPETÊNCIA FINALIZADO")
    print("=" * 70)


# ============================================================
# PROCESSAR DIMENSÃO
# ============================================================

def carregar_dimensao(
    config,
    metadados
):
    arquivo = config["arquivo"]
    tabela = config["tabela"]
    batch_size = config["batch_size"]
    etag = metadados["etag"]

    connection = get_oracle_connection()
    iniciou = False

    try:
        if carga_ja_realizada(
            connection,
            tabela,
            arquivo,
            etag
        ):
            print(
                f"{tabela}: ETag já processado. SKIP."
            )
            return

        iniciou = True

        with parquet_gold_temporario(
            arquivo
        ) as caminho:
            total_parquet = merge_parquet_em_batches(
                connection=connection,
                config=config,
                caminho_parquet=caminho,
                batch_size=batch_size
            )

        registrar_carga(
            connection=connection,
            tabela=tabela,
            arquivo=arquivo,
            etag=etag,
            qt_registros=total_parquet,
            status="SUCESSO"
        )

        connection.commit()

        print(
            f"{tabela}: MERGE concluído | "
            f"{total_parquet:,} linha(s) processadas."
        )

    except Exception:
        connection.rollback()

        if iniciou:
            try:
                registrar_carga(
                    connection=connection,
                    tabela=tabela,
                    arquivo=arquivo,
                    etag=etag,
                    qt_registros=None,
                    status="FALHA"
                )

                connection.commit()

            except Exception:
                connection.rollback()

        raise

    finally:
        connection.close()


# ============================================================
# PROCESSAR TABELA TEMPORAL
# ============================================================

def carregar_tabela_temporal(
    config,
    metadados
):
    arquivo = config["arquivo"]
    tabela = config["tabela"]
    batch_size = config["batch_size"]
    estrategia = config["estrategia"]
    etag = metadados["etag"]

    connection = get_oracle_connection()

    competencias_alteradas = []
    metadados_comp = {}

    try:
        # ----------------------------------------------------
        # 1. ETAG GLOBAL IGUAL -> NÃO BAIXA O PARQUET
        # ----------------------------------------------------

        if carga_ja_realizada(
            connection,
            tabela,
            arquivo,
            etag
        ):
            print(
                f"{tabela}: ETag já processado. SKIP."
            )
            return

        # ----------------------------------------------------
        # 2. PROTEÇÃO DE MIGRAÇÃO
        # ----------------------------------------------------

        total_atual = contar_registros(
            connection,
            tabela
        )

        possui_controle_comp = (
            existe_controle_competencia_tabela(
                connection,
                tabela
            )
        )

        if (
            total_atual > 0
            and not possui_controle_comp
        ):
            raise RuntimeError(
                f"{tabela} contém {total_atual:,} registros, "
                "mas ainda não possui controle por competência. "
                "Execute --bootstrap-competences primeiro."
            )

        # ----------------------------------------------------
        # 3. BAIXAR UMA VEZ E CALCULAR HASH POR COMPETÊNCIA
        # ----------------------------------------------------

        with parquet_gold_temporario(
            arquivo
        ) as caminho:
            metadados_comp = (
                analisar_parquet_por_competencia(
                    caminho,
                    config
                )
            )

            total_parquet = sum(
                meta["qt_registros"]
                for meta in metadados_comp.values()
            )

            controles = (
                obter_controles_competencias_sucesso(
                    connection,
                    tabela
                )
            )

            contagens_oracle = (
                obter_contagens_oracle_por_competencia(
                    connection,
                    config
                )
            )

            # ------------------------------------------------
            # 4. DECIDIR O QUE REALMENTE MUDOU
            # ------------------------------------------------

            print(
                "\nDecisão por competência:"
            )

            for competencia, meta in metadados_comp.items():
                controle = controles.get(
                    competencia
                )

                qt_oracle = contagens_oracle.get(
                    competencia,
                    0
                )

                hash_igual = (
                    controle is not None
                    and controle["hash"] == meta["hash"]
                )

                controle_qt_igual = (
                    controle is not None
                    and controle["qt_registros"]
                    == meta["qt_registros"]
                )

                oracle_qt_igual = (
                    qt_oracle
                    == meta["qt_registros"]
                )

                if (
                    total_atual > 0
                    and hash_igual
                    and controle_qt_igual
                    and oracle_qt_igual
                ):
                    print(
                        f"  {competencia} -> SKIP "
                        "(hash e contagem iguais)"
                    )
                    continue

                competencias_alteradas.append(
                    competencia
                )

                if controle is None:
                    motivo = "nova competência"
                elif not hash_igual:
                    motivo = "conteúdo alterado"
                elif not oracle_qt_igual:
                    motivo = "contagem Oracle divergente"
                else:
                    motivo = "controle divergente"

                print(
                    f"  {competencia} -> ATUALIZAR "
                    f"({motivo})"
                )

            # ------------------------------------------------
            # 5. SE NADA MUDOU, SÓ REGISTRA NOVO ETAG
            # ------------------------------------------------

            if not competencias_alteradas:
                registrar_carga(
                    connection=connection,
                    tabela=tabela,
                    arquivo=arquivo,
                    etag=etag,
                    qt_registros=total_parquet,
                    status="SUCESSO"
                )

                connection.commit()

                print(
                    f"{tabela}: conteúdo por competência "
                    "não mudou. Novo ETag registrado sem DML."
                )

                return

            # ------------------------------------------------
            # 6. SUBSTITUIR OU MERGEAR SÓ OS MESES ALTERADOS
            # ------------------------------------------------

            if estrategia == "replace_competencia":
                deletar_competencias(
                    connection=connection,
                    config=config,
                    competencias=competencias_alteradas
                )

                processados = (
                    inserir_competencias_em_batches(
                        connection=connection,
                        config=config,
                        caminho_parquet=caminho,
                        competencias=competencias_alteradas,
                        batch_size=batch_size
                    )
                )

            elif estrategia == "merge_competencia":
                processados = (
                    merge_competencias_em_batches(
                        connection=connection,
                        config=config,
                        caminho_parquet=caminho,
                        competencias=competencias_alteradas,
                        batch_size=batch_size
                    )
                )

            else:
                raise ValueError(
                    f"Estratégia temporal desconhecida: "
                    f"{estrategia}"
                )

            # ------------------------------------------------
            # 7. VALIDAR QUANTIDADE PROCESSADA
            # ------------------------------------------------

            for competencia in competencias_alteradas:
                esperado = metadados_comp[
                    competencia
                ]["qt_registros"]

                recebido = processados.get(
                    competencia,
                    0
                )

                if recebido != esperado:
                    raise RuntimeError(
                        f"{tabela} {competencia}: "
                        f"Gold={esperado:,} | "
                        f"processado={recebido:,}"
                    )

            contagens_depois = (
                obter_contagens_oracle_por_competencia(
                    connection,
                    config
                )
            )

            for competencia in competencias_alteradas:
                esperado = metadados_comp[
                    competencia
                ]["qt_registros"]

                encontrado = contagens_depois.get(
                    competencia,
                    0
                )

                if estrategia == "replace_competencia":
                    valido = (
                        encontrado == esperado
                    )
                else:
                    # UNIDADE_SAUDE usa MERGE porque possui filhos.
                    # Pode haver linha antiga ainda referenciada; por isso
                    # exigimos que todas as linhas da Gold estejam presentes,
                    # mas não deletamos versões atuais automaticamente.
                    valido = (
                        encontrado >= esperado
                    )

                if not valido:
                    raise RuntimeError(
                        f"{tabela} {competencia}: "
                        f"Gold={esperado:,} | "
                        f"Oracle após carga={encontrado:,}"
                    )

            # ------------------------------------------------
            # 8. REGISTRAR CONTROLE POR COMPETÊNCIA
            # ------------------------------------------------

            for competencia in competencias_alteradas:
                meta = metadados_comp[
                    competencia
                ]

                registrar_carga_competencia(
                    connection=connection,
                    tabela=tabela,
                    competencia=competencia_para_datetime(
                        competencia
                    ),
                    hash_competencia=meta["hash"],
                    qt_registros=meta["qt_registros"],
                    etag_arquivo=etag,
                    status="SUCESSO"
                )

            # ------------------------------------------------
            # 9. REGISTRAR ETAG GLOBAL
            # ------------------------------------------------

            registrar_carga(
                connection=connection,
                tabela=tabela,
                arquivo=arquivo,
                etag=etag,
                qt_registros=total_parquet,
                status="SUCESSO"
            )

            # ------------------------------------------------
            # 10. COMMIT ÚNICO
            # ------------------------------------------------

            connection.commit()

            print(
                f"\n{tabela}: sincronização concluída."
            )

            print(
                "Competências atualizadas: "
                + ", ".join(
                    competencias_alteradas
                )
            )

    except Exception:
        connection.rollback()

        print(
            f"\n{tabela}: ROLLBACK realizado."
        )

        try:
            for competencia in competencias_alteradas:
                if competencia not in metadados_comp:
                    continue

                meta = metadados_comp[
                    competencia
                ]

                registrar_carga_competencia(
                    connection=connection,
                    tabela=tabela,
                    competencia=competencia_para_datetime(
                        competencia
                    ),
                    hash_competencia=meta["hash"],
                    qt_registros=meta["qt_registros"],
                    etag_arquivo=etag,
                    status="FALHA"
                )

            registrar_carga(
                connection=connection,
                tabela=tabela,
                arquivo=arquivo,
                etag=etag,
                qt_registros=None,
                status="FALHA"
            )

            connection.commit()

        except Exception:
            connection.rollback()

        raise

    finally:
        connection.close()


# ============================================================
# CARREGAR TABELA
# ============================================================

def carregar_tabela(
    config,
    metadados
):
    tabela = config["tabela"]
    arquivo = config["arquivo"]
    estrategia = config["estrategia"]

    print("\n" + "=" * 70)
    print(f"PROCESSANDO {tabela}")
    print("=" * 70)

    print(
        f"Arquivo: {arquivo}"
    )

    print(
        f"ETag: {metadados['etag']}"
    )

    print(
        f"Estratégia: {estrategia}"
    )

    if estrategia == "merge_dimensao":
        carregar_dimensao(
            config,
            metadados
        )

    elif estrategia in {
        "replace_competencia",
        "merge_competencia"
    }:
        carregar_tabela_temporal(
            config,
            metadados
        )

    else:
        raise ValueError(
            f"Estratégia desconhecida em {tabela}: "
            f"{estrategia}"
        )


# ============================================================
# LIMPEZA DA JANELA DE RETENÇÃO
# ============================================================

def executar_cleanup_retencao():
    limite = obter_limite_retencao()

    print("\n" + "=" * 70)
    print("LIMPEZA DA JANELA MÓVEL")
    print("=" * 70)

    print(
        f"Retenção: {RETENCAO_MESES} meses completos"
    )

    print(
        "Dados com competência anterior a "
        f"{limite:%Y-%m-01} serão removidos."
    )

    # Filhos primeiro; UNIDADE_SAUDE por último.
    ordem = [
        "INTERNACAO",
        "FATO_EQUIPAMENTOS",
        "FATO_LEITOS",
        "PROFISSIONAL",
        "ALVARA",
        "UNIDADE_SAUDE",
    ]

    connection = get_oracle_connection()

    try:
        total_removido = 0

        for tabela in ordem:
            config = TABELAS_POR_NOME[
                tabela
            ]

            removidos = remover_fora_retencao(
                connection=connection,
                config=config,
                limite=limite
            )

            total_removido += removidos

        connection.commit()

        print(
            f"\nCleanup concluído: "
            f"{total_removido:,} linha(s) removidas."
        )

        print(
            "DIM_DATA e DIM_MUNICIPIO foram preservadas."
        )

    except Exception:
        connection.rollback()

        print(
            "\nCleanup falhou. "
            "ROLLBACK de toda a limpeza."
        )

        raise

    finally:
        connection.close()


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="SyntheSUS DB Loader"
    )

    grupo = (
        parser.add_mutually_exclusive_group(
            required=True
        )
    )

    grupo.add_argument(
        "--all",
        action="store_true",
        help=(
            "Sincroniza todas as tabelas e, ao final, "
            "executa a retenção de 12 meses"
        )
    )

    grupo.add_argument(
        "--table",
        type=str,
        help="Sincroniza apenas uma tabela"
    )

    grupo.add_argument(
        "--bootstrap-control",
        action="store_true",
        help=(
            "Registra o estado global atual do Oracle "
            "como correspondente à Gold atual"
        )
    )

    grupo.add_argument(
        "--bootstrap-competences",
        action="store_true",
        help=(
            "Cria o controle por competência para tabelas "
            "já carregadas, sem reinserir dados"
        )
    )

    grupo.add_argument(
        "--cleanup-retention",
        action="store_true",
        help=(
            "Remove dados anteriores à janela móvel "
            "de 12 meses, respeitando a ordem das FKs"
        )
    )

    args = parser.parse_args()

    print("=" * 70)
    print("SyntheSUS DB Loader")
    print("=" * 70)

    # ========================================================
    # 1. ORACLE
    # ========================================================

    testar_oracle()

    # ========================================================
    # 2. CLEANUP NÃO PRECISA ACESSAR OCI
    # ========================================================

    if args.cleanup_retention:
        executar_cleanup_retencao()
        return

    # ========================================================
    # 3. OCI
    # ========================================================

    objetos_gold = listar_objetos_gold()

    objetos_por_nome = {
        objeto["nome"]: objeto
        for objeto in objetos_gold
    }

    print(
        f"OCI Object Storage OK | "
        f"{len(objetos_gold)} objetos"
    )

    # ========================================================
    # 4. BOOTSTRAPS
    # ========================================================

    if args.bootstrap_control:
        bootstrap_controle(
            objetos_por_nome
        )
        return

    if args.bootstrap_competences:
        bootstrap_competencias(
            objetos_por_nome
        )
        return

    # ========================================================
    # 5. DEFINIR TABELAS
    # ========================================================

    if args.all:
        tabelas = TABELAS

    else:
        nome_tabela = (
            args.table.upper()
        )

        if nome_tabela not in TABELAS_POR_NOME:
            raise ValueError(
                f"Tabela desconhecida: "
                f"{nome_tabela}"
            )

        tabelas = [
            TABELAS_POR_NOME[
                nome_tabela
            ]
        ]

    # ========================================================
    # 6. PROCESSAR
    # ========================================================

    for config in tabelas:
        arquivo = config["arquivo"]

        if arquivo not in objetos_por_nome:
            raise FileNotFoundError(
                f"{arquivo} não encontrado "
                "no bucket Gold."
            )

        carregar_tabela(
            config=config,
            metadados=objetos_por_nome[
                arquivo
            ]
        )

    # ========================================================
    # 7. EM --all, LIMPA A RETENÇÃO SÓ DEPOIS DAS CARGAS
    # ========================================================

    if args.all:
        executar_cleanup_retencao()

    print("\n" + "=" * 70)
    print("PROCESSAMENTO FINALIZADO")
    print("=" * 70)


if __name__ == "__main__":
    main()