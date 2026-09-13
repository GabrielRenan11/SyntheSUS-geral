import os
from dotenv import load_dotenv


load_dotenv()


# ============================================================
# ORACLE
# ============================================================

ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DSN = os.getenv("ORACLE_DSN")

ORACLE_WALLET_DIR = os.getenv(
    "ORACLE_WALLET_DIR",
    "/opt/oracle/wallet"
)

ORACLE_WALLET_PASSWORD = os.getenv(
    "ORACLE_WALLET_PASSWORD"
)


# ============================================================
# OCI OBJECT STORAGE
# ============================================================

OCI_CONFIG_FILE = os.getenv(
    "OCI_CONFIG_FILE",
    "/root/.oci/config"
)

OCI_PROFILE = os.getenv(
    "OCI_PROFILE",
    "DEFAULT"
)

OCI_BUCKET_GOLD = os.getenv(
    "OCI_BUCKET_GOLD"
)

#===========================================================
# RETENÇÃO CUSTOMIZAVEL
#===========================================================
RETENCAO_MESES = 12