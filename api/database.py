import os
import threading

import oracledb
from dotenv import load_dotenv


load_dotenv()


# ============================================================
# CONNECTION POOL
# ============================================================

_pool = None
_pool_lock = threading.Lock()


def obter_pool():
    """
    Cria o pool apenas uma vez e o reutiliza durante
    toda a vida da aplicação.
    """

    global _pool

    if _pool is None:

        with _pool_lock:

            # Segunda verificação evita criação duplicada
            # se duas requisições chegarem simultaneamente.
            if _pool is None:

                print("Criando pool de conexões Oracle...")

                _pool = oracledb.create_pool(
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASSWORD"),
                    dsn=os.getenv("DB_SERVICE"),

                    config_dir=os.getenv("DB_WALLET_PATH"),
                    wallet_location=os.getenv("DB_WALLET_PATH"),
                    wallet_password=os.getenv("DB_WALLET_PASSWORD"),

                    min=1,
                    max=5,
                    increment=1,

                    # Verifica periodicamente se conexões
                    # paradas no pool continuam válidas.
                    ping_interval=60
                )

                print("Pool Oracle criado com sucesso.")

    return _pool


# ============================================================
# OBTER CONEXÃO
# ============================================================

def conectar_oracle():
    """
    Obtém uma conexão já existente do pool.

    Quando conexao.close() for chamado no restante da API,
    a conexão NÃO será destruída:
    ela será devolvida ao pool.
    """

    return obter_pool().acquire()