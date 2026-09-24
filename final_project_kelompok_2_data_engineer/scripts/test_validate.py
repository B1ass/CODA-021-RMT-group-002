# =========================================================
# this code created for testing great expectation code success or not
# without change or running in terminal, created only use for
# checking the transformation looks like, this
# test validate file only running in local visual 
# studio code by pressing the run button
# =========================================================

import os

from extract import load_data
from transform import transform_data
from validate import validate_data


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_PATH = os.path.join(
    BASE_DIR,
    "data",
    "Big_Black_Money_Dataset.csv"
)


print("=== EXTRACT ===")
df = load_data(DATA_PATH)

print("Jumlah data awal:", df.count())

print("\n=== TRANSFORM ===")
df = transform_data(df)

print("Jumlah data setelah transform:", df.count())

print("\n=== VALIDATION ===")
result = validate_data(df)

if not result:
    raise Exception("Great Expectations validation gagal.")

print("\n=== TEST VALIDATION BERHASIL ===")