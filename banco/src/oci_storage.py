import os
import tempfile
from contextlib import contextmanager

import oci

from config import (
    OCI_CONFIG_FILE,
    OCI_PROFILE,
    OCI_BUCKET_GOLD
)


# ============================================================
# CLIENTE OCI OBJECT STORAGE
# ============================================================

def get_object_storage_client():

    config = oci.config.from_file(
        file_location=OCI_CONFIG_FILE,
        profile_name=OCI_PROFILE
    )

    client = oci.object_storage.ObjectStorageClient(
        config
    )

    return client


# ============================================================
# NAMESPACE OCI
# ============================================================

def get_namespace(client):

    return client.get_namespace().data


# ============================================================
# LISTAR OBJETOS DO BUCKET GOLD COM METADADOS
# ============================================================

def listar_objetos_gold():

    client = get_object_storage_client()

    namespace = get_namespace(
        client
    )

    response = client.list_objects(
        namespace_name=namespace,
        bucket_name=OCI_BUCKET_GOLD,
        fields="name,etag,size,timeModified"
    )

    objetos = []

    for objeto in response.data.objects:

        objetos.append({
            "nome": objeto.name,
            "etag": objeto.etag,
            "tamanho": objeto.size,
            "ultima_modificacao": objeto.time_modified
        })

    return objetos


# ============================================================
# LISTAR SOMENTE OS NOMES
#
# Mantido por compatibilidade com o main.py atual.
# ============================================================

def listar_arquivos_gold():

    objetos = listar_objetos_gold()

    return [
        objeto["nome"]
        for objeto in objetos
    ]


# ============================================================
# OBTER METADADOS DE UM OBJETO ESPECÍFICO
# ============================================================

def obter_metadados_objeto(nome_arquivo):

    objetos = listar_objetos_gold()

    for objeto in objetos:

        if objeto["nome"] == nome_arquivo:
            return objeto

    raise FileNotFoundError(
        f"{nome_arquivo} não encontrado "
        f"no bucket Gold."
    )


# ============================================================
# PARQUET TEMPORÁRIO
# ============================================================

@contextmanager
def parquet_gold_temporario(nome_arquivo):

    client = get_object_storage_client()

    namespace = get_namespace(
        client
    )

    print(
        f"Baixando {nome_arquivo}..."
    )

    response = client.get_object(
        namespace_name=namespace,
        bucket_name=OCI_BUCKET_GOLD,
        object_name=nome_arquivo
    )

    arquivo_temporario = tempfile.NamedTemporaryFile(
        suffix=".parquet",
        delete=False
    )

    caminho = arquivo_temporario.name

    try:

        # ----------------------------------------------------
        # DOWNLOAD EM STREAMING
        # ----------------------------------------------------

        for chunk in response.data.raw.stream(
            1024 * 1024,
            decode_content=False
        ):

            arquivo_temporario.write(
                chunk
            )

        arquivo_temporario.close()

        print(
            f"{nome_arquivo} disponível temporariamente."
        )

        yield caminho

    finally:

        try:
            arquivo_temporario.close()

        except Exception:
            pass

        if os.path.exists(caminho):

            os.remove(
                caminho
            )

            print(
                f"Arquivo temporário removido: "
                f"{nome_arquivo}"
            )