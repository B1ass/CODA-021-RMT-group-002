import os

from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValuesToBeInSet,
)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(
    BASE_DIR,
    "data",
    "Big_Black_Money_Dataset.csv"
)

REQUIRED_COLUMNS = [
    "transaction_id",
    "country",
    "amount_usd",
    "transaction_type",
    "date_of_transaction",
    "person_involved",
    "industry",
    "destination_country",
    "reported_by_authority",
    "source_of_money",
    "money_laundering_risk_score",
    "shell_companies_involved",
    "financial_institution",
    "tax_haven_country",
]


def validate_data(df):
    """
    Melakukan validasi kualitas data Big Black Money
    menggunakan Great Expectations.
    """

    print("\n=== GREAT EXPECTATIONS VALIDATION ===")

    # buat GX Data Source dari Spark DataFrame
    context = gx.get_context()

    datasource = context.data_sources.add_spark(
        name="big_black_money_spark"
    )

    data_asset = datasource.add_dataframe_asset(
        name="big_black_money_data"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "big_black_money_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    # Membuat expectation suite
    suite = ExpectationSuite(
        name="big_black_money_suite"
    )

    # 1. Transaction ID Validation
    # Format "TX" + 10 digit angka (total 12 karakter)

    suite.add_expectation(
        ExpectColumnValuesToMatchRegex(
            column="transaction_id",
            regex=r"^TX\d{10}$"
        )
    )

    # Transaction ID tidak boleh NULL
    suite.add_expectation(
        ExpectColumnValuesToNotBeNull(
            column="transaction_id"
        )
    )

    # 2. Unique Transaction ID Validation

    suite.add_expectation(
        ExpectColumnValuesToBeUnique(
            column="transaction_id"
        )
    )

    # 3. Missing Value Validation
    # Seluruh kolom yang diperlukan tidak boleh null/kosong

    for column in REQUIRED_COLUMNS:
        suite.add_expectation(
            ExpectColumnValuesToNotBeNull(
                column=column
            )
        )

    # 4. Transaction Amount Validation
    # Amount (USD) harus angka positif > 0

    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="amount_usd",
            min_value=0,
            strict_min=True
        )
    )

    # 5. Money Laundering Risk Score Validation
    # Rentang 1-10

    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="money_laundering_risk_score",
            min_value=1,
            max_value=10
        )
    )

    # 6. Shell Companies Involved Validation
    # Rentang 0-9

    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="shell_companies_involved",
            min_value=0,
            max_value=9
        )
    )

    # 7. Source of Money Validation
    # Hanya "Legal" atau "Illegal"

    suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="source_of_money",
            value_set=["Legal", "Illegal"]
        )
    )

    # 8. Reported by Authority Validation
    # Hanya True / False

    suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="reported_by_authority",
            value_set=[True, False]
        )
    )

    # 9. Person Involved Format Validation
    # Format "Person" + digit angka

    suite.add_expectation(
        ExpectColumnValuesToMatchRegex(
            column="person_involved",
            regex=r"^Person_\d+$"
        )
    )

    # 10. Financial Institution Format Validation
    # Format "Bank" + digit angka

    suite.add_expectation(
        ExpectColumnValuesToMatchRegex(
            column="financial_institution",
            regex=r"^Bank_\d+$"
        )
    )

    # jalankan validation
    validation_result = batch.validate(
        suite
    )

    print("\n=== HASIL VALIDASI GX ===")
    print(validation_result)

    if validation_result.success:
        print("\n✓ Semua validasi GX BERHASIL.")
        return True
    else:
        print("\n✗ Validasi GX GAGAL.")
        return False
