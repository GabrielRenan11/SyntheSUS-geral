# ============================================================
# CONTROLE DE CARGA GLOBAL - ETAG DO ARQUIVO GOLD
# ============================================================

def carga_ja_realizada(
    connection,
    tabela,
    arquivo,
    etag
):
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM CONTROLE_CARGA_GOLD
            WHERE TABELA = :tabela
              AND ARQUIVO = :arquivo
              AND ETAG = :etag
              AND STATUS = 'SUCESSO'
            """,
            tabela=tabela,
            arquivo=arquivo,
            etag=etag
        )

        quantidade = cursor.fetchone()[0]
        return quantidade > 0

    finally:
        cursor.close()


def registrar_carga(
    connection,
    tabela,
    arquivo,
    etag,
    qt_registros,
    status
):
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO CONTROLE_CARGA_GOLD (
                TABELA,
                ARQUIVO,
                ETAG,
                QT_REGISTROS,
                STATUS
            )
            VALUES (
                :tabela,
                :arquivo,
                :etag,
                :qt_registros,
                :status
            )
            """,
            tabela=tabela,
            arquivo=arquivo,
            etag=etag,
            qt_registros=qt_registros,
            status=status
        )

    finally:
        cursor.close()


def obter_ultima_carga_sucesso(
    connection,
    tabela,
    arquivo
):
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                ETAG,
                DATA_CARGA,
                QT_REGISTROS
            FROM CONTROLE_CARGA_GOLD
            WHERE TABELA = :tabela
              AND ARQUIVO = :arquivo
              AND STATUS = 'SUCESSO'
            ORDER BY DATA_CARGA DESC
            FETCH FIRST 1 ROW ONLY
            """,
            tabela=tabela,
            arquivo=arquivo
        )

        resultado = cursor.fetchone()

        if resultado is None:
            return None

        return {
            "etag": resultado[0],
            "data_carga": resultado[1],
            "qt_registros": resultado[2]
        }

    finally:
        cursor.close()


# ============================================================
# CONTROLE POR COMPETÊNCIA
# ============================================================

def existe_controle_competencia_tabela(
    connection,
    tabela
):
    """
    Retorna True quando a tabela já possui ao menos uma
    competência registrada com SUCESSO.
    """
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM CONTROLE_CARGA_COMPETENCIA
            WHERE TABELA = :tabela
              AND STATUS = 'SUCESSO'
            """,
            tabela=tabela
        )

        return cursor.fetchone()[0] > 0

    finally:
        cursor.close()


def obter_controles_competencias_sucesso(
    connection,
    tabela
):
    """
    Retorna somente o registro de SUCESSO mais recente
    de cada competência.

    Saída:
        {
            "202608": {
                "hash": "...",
                "qt_registros": 123,
                "etag": "...",
                "data_carga": ...
            }
        }
    """
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COMPETENCIA,
                HASH_COMPETENCIA,
                QT_REGISTROS,
                ETAG_ARQUIVO,
                DATA_CARGA
            FROM (
                SELECT
                    COMPETENCIA,
                    HASH_COMPETENCIA,
                    QT_REGISTROS,
                    ETAG_ARQUIVO,
                    DATA_CARGA,
                    ROW_NUMBER() OVER (
                        PARTITION BY COMPETENCIA
                        ORDER BY DATA_CARGA DESC, ID_CARGA DESC
                    ) AS RN
                FROM CONTROLE_CARGA_COMPETENCIA
                WHERE TABELA = :tabela
                  AND STATUS = 'SUCESSO'
            )
            WHERE RN = 1
            ORDER BY COMPETENCIA
            """,
            tabela=tabela
        )

        controles = {}

        for (
            competencia,
            hash_competencia,
            qt_registros,
            etag_arquivo,
            data_carga
        ) in cursor.fetchall():

            chave = competencia.strftime("%Y%m")

            controles[chave] = {
                "hash": hash_competencia,
                "qt_registros": int(qt_registros),
                "etag": etag_arquivo,
                "data_carga": data_carga
            }

        return controles

    finally:
        cursor.close()


def competencia_ja_sincronizada(
    connection,
    tabela,
    competencia,
    hash_competencia
):
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM CONTROLE_CARGA_COMPETENCIA
            WHERE TABELA = :tabela
              AND COMPETENCIA = :competencia
              AND HASH_COMPETENCIA = :hash_competencia
              AND STATUS = 'SUCESSO'
            """,
            tabela=tabela,
            competencia=competencia,
            hash_competencia=hash_competencia
        )

        return cursor.fetchone()[0] > 0

    finally:
        cursor.close()


def registrar_carga_competencia(
    connection,
    tabela,
    competencia,
    hash_competencia,
    qt_registros,
    etag_arquivo,
    status
):
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO CONTROLE_CARGA_COMPETENCIA (
                TABELA,
                COMPETENCIA,
                HASH_COMPETENCIA,
                QT_REGISTROS,
                ETAG_ARQUIVO,
                STATUS
            )
            VALUES (
                :tabela,
                :competencia,
                :hash_competencia,
                :qt_registros,
                :etag_arquivo,
                :status
            )
            """,
            tabela=tabela,
            competencia=competencia,
            hash_competencia=hash_competencia,
            qt_registros=qt_registros,
            etag_arquivo=etag_arquivo,
            status=status
        )

    finally:
        cursor.close()