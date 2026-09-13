import oracledb

from config import (
    ORACLE_USER,
    ORACLE_PASSWORD,
    ORACLE_DSN,
    ORACLE_WALLET_DIR,
    ORACLE_WALLET_PASSWORD
)


def get_oracle_connection():

    return oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN,
        config_dir=ORACLE_WALLET_DIR,
        wallet_location=ORACLE_WALLET_DIR,
        wallet_password=ORACLE_WALLET_PASSWORD
    )