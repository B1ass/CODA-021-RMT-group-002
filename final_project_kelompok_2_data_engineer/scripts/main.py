import os

from extract import load_data
from transform import (
    transform_data,
    create_dimensions,
    create_fact_transaction
)
from validate import validate_data
from load import load_to_postgres, clear_postgres_tables


# =========================================================
# KONFIGURASI
# =========================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_PATH = os.path.join(
    BASE_DIR,
    "data",
    "Big_Black_Money_Dataset.csv"
)


# =========================================================
# EXTRACT
# =========================================================

print("\n=== EXTRACT ===")

df = load_data(DATA_PATH)

print("Jumlah data awal:", df.count())

df.show(
    5,
    truncate=False
)


# =========================================================
# TRANSFORM
# =========================================================

print("\n=== TRANSFORM ===")

df = transform_data(df)

print(
    "Jumlah data setelah transform:",
    df.count()
)

df.show(
    5,
    truncate=False
)


# =========================================================
# GREAT EXPECTATIONS VALIDATION
# =========================================================

print("\n=== GREAT EXPECTATIONS ===")

validation_success = validate_data(df)

if not validation_success:
    raise Exception(
        "Great Expectations validation gagal. "
        "Pipeline dihentikan sebelum proses load."
    )

print(
    "\n✓ Great Expectations validation berhasil."
)


# =========================================================
# CREATE DIMENSIONS
# =========================================================

print("\n=== CREATE DIMENSIONS ===")

dimensions = create_dimensions(df)

for name, dimension in dimensions.items():
    print(
        f"{name}: {dimension.count()} rows"
    )


# =========================================================
# CREATE FACT TRANSACTION
# =========================================================

print("\n=== CREATE FACT TRANSACTION ===")

fact_df = create_fact_transaction(
    df,
    dimensions
)

print(
    "Jumlah fact transaction:",
    fact_df.count()
)

fact_df.show(
    10,
    truncate=False
)


# =========================================================
# FACT SCHEMA
# =========================================================

print("\n=== FACT SCHEMA ===")

fact_df.printSchema()


# =========================================================
# CEK FOREIGN KEY NULL
# =========================================================

print("\n=== CEK FOREIGN KEY NULL ===")

from pyspark.sql.functions import col, sum, when

fact_df.select(
    sum(
        when(
            col("date_key").isNull(),
            1
        ).otherwise(0)
    ).alias("date_key_null"),

    sum(
        when(
            col("country_key").isNull(),
            1
        ).otherwise(0)
    ).alias("country_key_null"),

    sum(
        when(
            col("destination_country_key").isNull(),
            1
        ).otherwise(0)
    ).alias("destination_country_key_null"),

    sum(
        when(
            col("tax_haven_country_key").isNull(),
            1
        ).otherwise(0)
    ).alias("tax_haven_country_key_null"),

    sum(
        when(
            col("person_key").isNull(),
            1
        ).otherwise(0)
    ).alias("person_key_null"),

    sum(
        when(
            col("transaction_type_key").isNull(),
            1
        ).otherwise(0)
    ).alias("transaction_type_key_null"),

    sum(
        when(
            col("industry_key").isNull(),
            1
        ).otherwise(0)
    ).alias("industry_key_null"),

    sum(
        when(
            col("financial_institution_key").isNull(),
            1
        ).otherwise(0)
    ).alias("financial_institution_key_null"),

    sum(
        when(
            col("source_money_key").isNull(),
            1
        ).otherwise(0)
    ).alias("source_money_key_null")
).show()


# =========================================================
# CEK DUPLIKASI TRANSACTION ID
# =========================================================

print("\n=== CEK DUPLIKASI TRANSACTION ID ===")

total_fact = fact_df.count()

unique_transaction = (
    fact_df
    .select("transaction_id")
    .distinct()
    .count()
)

print(
    "Total fact:",
    total_fact
)

print(
    "Unique transaction_id:",
    unique_transaction
)


# =========================================================
# LOAD KE POSTGRESQL
# =========================================================

print("\n=== LOAD KE POSTGRESQL ===")

clear_postgres_tables(fact_df.sparkSession)

for name, dimension in dimensions.items():
    print(f"\nLoading {name}...")
    load_to_postgres(dimension, name)

print("\nLoading fact_transaction...")
load_to_postgres(fact_df, "fact_transaction")

print("\n=== LOAD SELESAI ===")
print("\n=== PIPELINE BIG BLACK MONEY SELESAI ===")