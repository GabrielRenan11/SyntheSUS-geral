# Importação de bibliotecas
import asyncio
import io
import os
from datetime import datetime

import pandas as pd
import oci
from pysus.api.client import PySUS


# ============================================================
# CONFIGURAÇÕES
# ============================================================

# Configurações do OCI Object Storage
PATH_CONFIG = os.getenv(
    "OCI_CONFIG_FILE",
    "/root/.oci/config"
)

BUCKET_NAME = os.getenv(
    "BUCKET_BRONZE",
    "synthesus-bronze"
)


# Definição dos estados
ESTADOS_BR = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]


# ============================================================
# OBTENÇÃO DOS ÚLTIMOS 12 MESES COMPLETOS
# ============================================================

def obter_ultimos_12_meses():
    """
    Retorna os últimos 12 meses completos,
    terminando no mês anterior ao atual.

    Exemplo:

    Data atual: 09/2026

    Retorna:
    09/2025
    10/2025
    11/2025
    12/2025
    01/2026
    ...
    07/2026
    08/2026
    """

    hoje = datetime.now()

    # Começa pelo mês passado
    mes_final = hoje.month - 1
    ano_final = hoje.year

    # Caso estejamos em janeiro,
    # o mês anterior é dezembro do ano anterior
    if mes_final == 0:
        mes_final = 12
        ano_final -= 1

    meses_alvo = []

    # Vai de 11 meses antes do mês final
    # até o próprio mês final
    for lag in range(11, -1, -1):

        mes = mes_final - lag
        ano = ano_final

        while mes <= 0:
            mes += 12
            ano -= 1

        meses_alvo.append(
            (ano, mes)
        )

    return meses_alvo


# ============================================================
# CLIENTE OCI
# ============================================================

def obter_cliente_oci():
    """
    Inicializa e retorna o cliente Object Storage
    da OCI e o namespace.
    """

    try:

        config = oci.config.from_file(
            PATH_CONFIG,
            "DEFAULT"
        )

        client = oci.object_storage.ObjectStorageClient(
            config
        )

        namespace = client.get_namespace().data

        print(
            "✓ Conectado à OCI com sucesso!"
        )

        return client, namespace

    except Exception as e:

        print(
            f"❌ Erro ao autenticar no OCI "
            f"usando {PATH_CONFIG}: {e}"
        )

        raise


# ============================================================
# LISTAR ARQUIVOS QUE JÁ EXISTEM NA BRONZE
# ============================================================

def obter_objetos_existentes(
    object_storage_client,
    namespace
):
    """
    Lista os objetos já existentes no bucket Bronze
    e retorna seus nomes em um set.

    Isso evita consultar a OCI individualmente para
    cada arquivo durante o pipeline.
    """

    print(
        "\n🔎 Verificando arquivos que já existem "
        "na Camada Bronze..."
    )

    objetos_existentes = set()

    start = None

    try:

        while True:

            parametros = {
                "namespace_name": namespace,
                "bucket_name": BUCKET_NAME
            }

            # Só envia o parâmetro start
            # a partir da segunda página
            if start is not None:
                parametros["start"] = start

            resposta = (
                object_storage_client.list_objects(
                    **parametros
                )
            )

            # Adiciona os nomes encontrados ao set
            for objeto in resposta.data.objects:

                objetos_existentes.add(
                    objeto.name
                )

            # Se não houver próxima página,
            # termina a listagem
            if not resposta.data.next_start_with:
                break

            start = resposta.data.next_start_with

        print(
            f"✓ {len(objetos_existentes):,} "
            f"objetos encontrados no bucket."
        )

        return objetos_existentes

    except Exception as e:

        print(
            f"❌ Erro ao listar objetos "
            f"da Camada Bronze: {e}"
        )

        raise


# ============================================================
# DOWNLOAD DATASUS + UPLOAD BRONZE
# ============================================================

async def baixar_e_enviar_bronze(
    client_pysus,
    object_storage_client,
    namespace,
    arquivos,
    prefixo,
    object_name,
    objetos_existentes
):
    """
    Verifica primeiro se o arquivo já existe na OCI.

    Se existir:
        pula completamente o download.

    Se não existir:
        baixa do DATASUS,
        converte para Parquet
        e envia para a Bronze.
    """

    # ========================================================
    # VERIFICAÇÃO NA OCI
    # ========================================================

    if object_name in objetos_existentes:

        print(
            f"  ⏭️ [IGNORADO] "
            f"'{object_name}' já existe na Bronze."
        )

        return


    # ========================================================
    # FILTRAGEM DO ARQUIVO DATASUS
    # ========================================================

    filtrados = [
        arquivo
        for arquivo in arquivos
        if arquivo.name.startswith(prefixo)
    ]

    if not filtrados:

        print(
            f"  ❌ Nenhum arquivo encontrado "
            f"para o prefixo '{prefixo}'."
        )

        return


    alvo = filtrados[0]

    print(
        f"  [DOWNLOAD] "
        f"Baixando {alvo.name} ({prefixo})..."
    )


    caminho_pysus = None
    buffer = None
    df_bruto = None


    try:

        # ----------------------------------------------------
        # DOWNLOAD VIA PYSUS
        # ----------------------------------------------------

        resultado = (
            await client_pysus.download_to_parquet(
                alvo
            )
        )

        caminho_pysus = (
            resultado.path
            if hasattr(resultado, "path")
            else str(resultado)
        )


        # ----------------------------------------------------
        # CARREGA OS DADOS
        # ----------------------------------------------------

        df_bruto = pd.read_parquet(
            caminho_pysus
        )

        print(
            f"  ✓ {len(df_bruto):,} "
            f"registros brutos carregados."
        )


        # ----------------------------------------------------
        # CONVERSÃO PARA BUFFER PARQUET
        # ----------------------------------------------------

        buffer = io.BytesIO()

        df_bruto.to_parquet(
            buffer,
            index=False
        )

        buffer.seek(0)


        # ----------------------------------------------------
        # UPLOAD PARA OCI
        # ----------------------------------------------------

        print(
            f"  [OCI] Enviando "
            f"'{object_name}' "
            f"para o bucket "
            f"'{BUCKET_NAME}'..."
        )

        object_storage_client.put_object(
            namespace_name=namespace,
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            put_object_body=buffer
        )


        print(
            f"  ✓ [SUCESSO OCI] "
            f"'{object_name}' salvo "
            f"na Camada Bronze!"
        )


        # ----------------------------------------------------
        # ADICIONA AO CACHE DE OBJETOS
        # ----------------------------------------------------

        # Assim, caso o mesmo objeto apareça novamente
        # durante a própria execução, ele será ignorado.

        objetos_existentes.add(
            object_name
        )


    except Exception as e:

        print(
            f"  ❌ [ERRO] "
            f"Falha ao processar "
            f"{object_name}: {e}"
        )


    finally:

        # ----------------------------------------------------
        # LIMPEZA DE MEMÓRIA
        # ----------------------------------------------------

        if buffer is not None:
            buffer.close()

        if df_bruto is not None:
            del df_bruto


        # ----------------------------------------------------
        # REMOVE ARQUIVO TEMPORÁRIO
        # ----------------------------------------------------

        if (
            caminho_pysus is not None
            and os.path.exists(caminho_pysus)
        ):

            try:

                os.remove(
                    caminho_pysus
                )

            except Exception as e:

                print(
                    f"  ⚠️ Não foi possível remover "
                    f"o arquivo temporário "
                    f"{caminho_pysus}: {e}"
                )


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

async def executar_pipeline_bronze():

    # --------------------------------------------------------
    # CONEXÃO OCI
    # --------------------------------------------------------

    (
        object_storage_client,
        namespace
    ) = obter_cliente_oci()


    # --------------------------------------------------------
    # LISTA OBJETOS QUE JÁ ESTÃO NA BRONZE
    # --------------------------------------------------------

    objetos_existentes = obter_objetos_existentes(
        object_storage_client,
        namespace
    )


    # --------------------------------------------------------
    # DEFINE OS 12 MESES
    # --------------------------------------------------------

    meses_processamento = (
        obter_ultimos_12_meses()
    )


    print(
        "\n"
        "======================================================="
    )

    print(
        "🇧🇷 PIPELINE BRONZE - DATASUS"
    )

    print(
        "======================================================="
    )


    print(
        "\n📅 Competências que serão processadas:"
    )

    for ano, mes in meses_processamento:

        print(
            f"  → {mes:02d}/{ano}"
        )


    print(
        f"\nTotal de competências: "
        f"{len(meses_processamento)}"
    )

    print(
        f"Total de UFs: "
        f"{len(ESTADOS_BR)}"
    )

    print(
        f"Máximo teórico de arquivos: "
        f"{len(meses_processamento) * len(ESTADOS_BR) * 5:,}"
    )


    # --------------------------------------------------------
    # CONEXÃO DATASUS
    # --------------------------------------------------------

    print(
        "\nConectando ao FTP "
        "do DATASUS via PySUS..."
    )


    try:

        client_pysus = PySUS()

        ftp = (
            await client_pysus.get_ftp()
        )

        datasets = (
            await ftp.datasets()
        )


        # Dataset SIH
        sih_ds = next(
            d
            for d in datasets
            if d.name == "SIH"
        )


        # Dataset CNES
        cnes_ds = next(
            d
            for d in datasets
            if d.name == "CNES"
        )


        # ====================================================
        # MESES
        # ====================================================

        for ano, mes in meses_processamento:

            print(
                "\n"
                "======================================================="
            )

            print(
                f"📅 PROCESSANDO COMPETÊNCIA: "
                f"{mes:02d}/{ano}"
            )

            print(
                "======================================================="
            )


            # ================================================
            # ESTADOS
            # ================================================

            for uf in ESTADOS_BR:

                uf_lower = uf.lower()

                print(
                    f"\n📌 Estado: "
                    f"{uf} "
                    f"({mes:02d}/{ano})"
                )


                try:

                    # ----------------------------------------
                    # BUSCA DATASUS
                    # ----------------------------------------

                    arquivos_sih = (
                        await sih_ds.search(
                            state=uf,
                            year=ano,
                            month=mes
                        )
                    )

                    arquivos_cnes = (
                        await cnes_ds.search(
                            state=uf,
                            year=ano,
                            month=mes
                        )
                    )


                    # ----------------------------------------
                    # FONTES
                    # ----------------------------------------

                    fontes_coleta = [

                        (
                            "RD (SIH - Internações)",
                            arquivos_sih,
                            "RD",
                            "raw_sih_rd"
                        ),

                        (
                            "ST (CNES - Cadastro)",
                            arquivos_cnes,
                            "ST",
                            "raw_cnes_st"
                        ),

                        (
                            "LT (CNES - Leitos)",
                            arquivos_cnes,
                            "LT",
                            "raw_cnes_lt"
                        ),

                        (
                            "PF (CNES - Profissionais)",
                            arquivos_cnes,
                            "PF",
                            "raw_cnes_pf"
                        ),

                        (
                            "EQ (CNES - Equipamentos)",
                            arquivos_cnes,
                            "EQ",
                            "raw_cnes_eq"
                        )
                    ]


                    # ========================================
                    # DOWNLOAD + UPLOAD
                    # ========================================

                    for (
                        rotulo,
                        lista_arquivos,
                        prefixo,
                        nome_base
                    ) in fontes_coleta:

                        print(
                            f"\n--> Coletando: "
                            f"{rotulo}"
                        )


                        object_name = (
                            f"{nome_base}_"
                            f"{uf_lower}_"
                            f"{ano}"
                            f"{mes:02d}.parquet"
                        )


                        await baixar_e_enviar_bronze(
                            client_pysus,
                            object_storage_client,
                            namespace,
                            lista_arquivos,
                            prefixo,
                            object_name,
                            objetos_existentes
                        )


                except Exception as e:

                    print(
                        f"❌ Erro ao processar "
                        f"{uf} "
                        f"({mes:02d}/{ano}): "
                        f"{e}"
                    )

                    print(
                        "➡ Continuando para "
                        "a próxima UF..."
                    )

                    continue


        # ====================================================
        # FINALIZAÇÃO
        # ====================================================

        print(
            "\n"
            "======================================================="
        )

        print(
            "✅ [PIPELINE CONCLUÍDO]"
        )

        print(
            "Últimos 12 meses completos finalizados!"
        )

        print(
            "======================================================="
        )


    except Exception as e:

        print(
            f"\n❌ [ERRO NO PIPELINE] "
            f"Falha durante a execução: {e}"
        )


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":

    asyncio.run(
        executar_pipeline_bronze()
    )