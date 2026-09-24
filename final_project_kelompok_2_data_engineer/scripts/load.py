import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Deteksi environment Airflow Docker
if os.path.exists("/opt/airflow/project"):
    POSTGRES_HOST = "192.168.65.254"

POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

POSTGRES_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
)

POSTGRES_DRIVER = "org.postgresql.Driver"

def validate_config():
    if not POSTGRES_DATABASE:
        raise ValueError("POSTGRES_DATABASE tidak ditemukan di .env")

    if not POSTGRES_USER:
        raise ValueError("POSTGRES_USER tidak ditemukan di .env")

    if not POSTGRES_PASSWORD:
        raise ValueError("POSTGRES_PASSWORD tidak ditemukan di .env")


def clear_postgres_tables(spark):
    """
    Menghapus seluruh data lama tanpa menghapus struktur tabel,
    primary key, dan foreign key.
    """

    validate_config()

    print("\n=== CLEAR DATA POSTGRESQL ===")

    spark._jvm.Class.forName(POSTGRES_DRIVER)

    connection = spark._jvm.java.sql.DriverManager.getConnection(
        POSTGRES_URL,
        POSTGRES_USER,
        POSTGRES_PASSWORD
    )

    statement = connection.createStatement()

    try:
        print("TRUNCATE semua tabel...")

        statement.execute("""
            TRUNCATE TABLE
                fact_transaction,
                dim_date,
                dim_country,
                dim_person,
                dim_transaction_type,
                dim_industry,
                dim_financial_institution,
                dim_source_money
        """)

        print("✓ Semua tabel berhasil dikosongkan.")

    finally:
        statement.close()
        connection.close()

def load_to_postgres(df, table_name):
    """
    Memasukkan DataFrame Spark ke tabel PostgreSQL.
    Tidak menggunakan overwrite agar PK/FK tidak terhapus.
    """

    validate_config()

    print(f"Loading {table_name} ke PostgreSQL...")

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )

    print(f"Berhasil load {table_name} ke PostgreSQL.")