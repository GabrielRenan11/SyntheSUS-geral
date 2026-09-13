import os
import oracledb
from dotenv import load_dotenv

load_dotenv()

try:
    connection = oracledb.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dsn=os.getenv("DB_SERVICE"),
        config_dir=os.getenv("DB_WALLET_PATH"),
        wallet_location=os.getenv("DB_WALLET_PATH"),
        wallet_password=os.getenv("DB_WALLET_PASSWORD")
    )

    print("✅ Conectado ao Oracle!")

    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM DUAL")
        print("Resultado:", cursor.fetchone()[0])

    connection.close()

except Exception as erro:
    print("❌ Erro:")
    print(erro)