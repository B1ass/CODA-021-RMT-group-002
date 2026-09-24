# =========================================================
# this code created for testing transform code without
# change or running in terminal, created only use for
# checking the transformation looks like, this
# test transformation file only running in local visual 
# studio code by pressing the run button
# =========================================================


import os

from extract import load_data
from transform import (
    transform_data,
    create_dimensions,
    create_fact_transaction
)

# =========================================================
# PATH DATASET
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

DATA_PATH = os.path.join(
    BASE_DIR,
    "data",
    "Big_Black_Money_Dataset.csv"
)


# =========================================================
# 1. EXTRACT
# =========================================================

df = load_data(DATA_PATH)

print("=== DATA SEBELUM TRANSFORM ===")
print(df.columns)


# =========================================================
# 2. TRANSFORM DASAR
# =========================================================

transformed_df = transform_data(df)

print("\n=== DATA SETELAH TRANSFORM ===")
print(transformed_df.columns)

print("\n=== JUMLAH DATA ===")
print(transformed_df.count())

print("\n=== SAMPLE DATA ===")
transformed_df.show(5, truncate=False)


# =========================================================
# 3. MEMBUAT 7 DIMENSION
# =========================================================

dimensions = create_dimensions(transformed_df)


# =========================================================
# 4. MENAMPILKAN DIMENSION
# =========================================================

print("\n=== DIM COUNTRY ===")
dimensions["dim_country"].show()

print("\n=== DIM PERSON ===")
dimensions["dim_person"].show(5)

print("\n=== DIM TRANSACTION TYPE ===")
dimensions["dim_transaction_type"].show()

print("\n=== DIM INDUSTRY ===")
dimensions["dim_industry"].show()

print("\n=== DIM FINANCIAL INSTITUTION ===")
dimensions["dim_financial_institution"].show(5)

print("\n=== DIM SOURCE MONEY ===")
dimensions["dim_source_money"].show()

print("\n=== DIM DATE ===")
dimensions["dim_date"].show(5)

print("\n=== FACT TRANSACTION ===")

fact_transaction = create_fact_transaction(
    transformed_df,
    dimensions
)

fact_transaction.show(10, truncate=False)

print("\n=== JUMLAH FACT TRANSACTION ===")
print(fact_transaction.count())

print("\n=== KOLOM FACT TRANSACTION ===")
print(fact_transaction.columns)