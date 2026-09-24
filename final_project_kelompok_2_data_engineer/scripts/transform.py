from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, to_date, year, month, dayofmonth, dayofweek, date_format
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def transform_data(df: DataFrame) -> DataFrame:

    # =========================================================
    # 1. RENAME COLUMN MENJADI SNAKE_CASE
    # =========================================================

    df = df.withColumnRenamed("Transaction ID", "transaction_id") \
           .withColumnRenamed("Country", "country") \
           .withColumnRenamed("Amount (USD)", "amount_usd") \
           .withColumnRenamed("Transaction Type", "transaction_type") \
           .withColumnRenamed("Date of Transaction", "date_of_transaction") \
           .withColumnRenamed("Person Involved", "person_involved") \
           .withColumnRenamed("Industry", "industry") \
           .withColumnRenamed("Destination Country", "destination_country") \
           .withColumnRenamed("Reported by Authority", "reported_by_authority") \
           .withColumnRenamed("Source of Money", "source_of_money") \
           .withColumnRenamed(
               "Money Laundering Risk Score",
               "money_laundering_risk_score"
           ) \
           .withColumnRenamed(
               "Shell Companies Involved",
               "shell_companies_involved"
           ) \
           .withColumnRenamed(
               "Financial Institution",
               "financial_institution"
           ) \
           .withColumnRenamed(
               "Tax Haven Country",
               "tax_haven_country"
           )


    # =========================================================
    # 2. MEMBERSIHKAN SPASI PADA DATA KATEGORIK
    # =========================================================

    categorical_columns = [
        "country",
        "transaction_type",
        "person_involved",
        "industry",
        "destination_country",
        "source_of_money",
        "financial_institution",
        "tax_haven_country"
    ]

    for column in categorical_columns:
        df = df.withColumn(
            column,
            trim(col(column))
        )


    # =========================================================
    # 3. FILTER TRANSACTION ID YANG NULL
    # =========================================================

    df = df.filter(
        col("transaction_id").isNotNull()
    )


    # =========================================================
    # 4. RETURN HASIL TRANSFORMASI DASAR
    # =========================================================

    return df

def create_dimensions(df: DataFrame):

    # =========================================================
    # 1. DIM COUNTRY
    # =========================================================

    country_df = (
        df.select(col("country").alias("country"))
        .union(
            df.select(col("destination_country").alias("country"))
        )
        .union(
            df.select(col("tax_haven_country").alias("country"))
        )
        .filter(col("country").isNotNull())
        .dropDuplicates()
    )

    country_window = Window.orderBy("country")

    dim_country_df = (
        country_df
        .withColumn(
            "country_key",
            row_number().over(country_window)
        )
        .select(
            "country_key",
            "country"
        )
    )


    # =========================================================
    # 2. DIM PERSON
    # =========================================================

    person_df = (
        df.select("person_involved")
        .filter(col("person_involved").isNotNull())
        .dropDuplicates()
    )

    person_window = Window.orderBy("person_involved")

    dim_person_df = (
        person_df
        .withColumn(
            "person_key",
            row_number().over(person_window)
        )
        .select(
            "person_key",
            "person_involved"
        )
    )


    # =========================================================
    # 3. DIM TRANSACTION TYPE
    # =========================================================

    transaction_type_df = (
        df.select("transaction_type")
        .filter(col("transaction_type").isNotNull())
        .dropDuplicates()
    )

    transaction_type_window = Window.orderBy("transaction_type")

    dim_transaction_type_df = (
        transaction_type_df
        .withColumn(
            "transaction_type_key",
            row_number().over(transaction_type_window)
        )
        .select(
            "transaction_type_key",
            "transaction_type"
        )
    )


    # =========================================================
    # 4. DIM INDUSTRY
    # =========================================================

    industry_df = (
        df.select("industry")
        .filter(col("industry").isNotNull())
        .dropDuplicates()
    )

    industry_window = Window.orderBy("industry")

    dim_industry_df = (
        industry_df
        .withColumn(
            "industry_key",
            row_number().over(industry_window)
        )
        .select(
            "industry_key",
            "industry"
        )
    )


    # =========================================================
    # 5. DIM FINANCIAL INSTITUTION
    # =========================================================

    financial_institution_df = (
        df.select("financial_institution")
        .filter(col("financial_institution").isNotNull())
        .dropDuplicates()
    )

    financial_institution_window = Window.orderBy(
        "financial_institution"
    )

    dim_financial_institution_df = (
        financial_institution_df
        .withColumn(
            "financial_institution_key",
            row_number().over(financial_institution_window)
        )
        .select(
            "financial_institution_key",
            "financial_institution"
        )
    )


    # =========================================================
    # 6. DIM SOURCE MONEY
    # =========================================================

    source_money_df = (
        df.select("source_of_money")
        .filter(col("source_of_money").isNotNull())
        .dropDuplicates()
    )

    source_money_window = Window.orderBy("source_of_money")

    dim_source_money_df = (
        source_money_df
        .withColumn(
            "source_money_key",
            row_number().over(source_money_window)
        )
        .select(
            "source_money_key",
            "source_of_money"
        )
    )


    # =========================================================
    # 7. DIM DATE
    # =========================================================

    date_df = (
        df.select(
            to_date("date_of_transaction").alias("full_date")
        )
        .filter(col("full_date").isNotNull())
        .dropDuplicates()
    )

    date_window = Window.orderBy("full_date")

    dim_date_df = (
        date_df
        .withColumn(
            "date_key",
            row_number().over(date_window)
        )
        .withColumn(
            "year",
            year("full_date")
        )
        .withColumn(
            "month",
            month("full_date")
        )
        .withColumn(
            "month_name",
            date_format("full_date", "MMMM")
        )
        .withColumn(
            "day",
            dayofmonth("full_date")
        )
        .withColumn(
            "day_of_week",
            dayofweek("full_date")
        )
        .select(
            "date_key",
            "full_date",
            "year",
            "month",
            "month_name",
            "day",
            "day_of_week"
        )
    )


    # =========================================================
    # RETURN 7 DIMENSION DATAFRAME
    # =========================================================

    return {
        "dim_date": dim_date_df,
        "dim_country": dim_country_df,
        "dim_person": dim_person_df,
        "dim_transaction_type": dim_transaction_type_df,
        "dim_industry": dim_industry_df,
        "dim_financial_institution": dim_financial_institution_df,
        "dim_source_money": dim_source_money_df
    }
def create_fact_transaction(df: DataFrame, dimensions: dict):

    # Ambil setiap dimension
    dim_country = dimensions["dim_country"]
    dim_person = dimensions["dim_person"]
    dim_transaction_type = dimensions["dim_transaction_type"]
    dim_industry = dimensions["dim_industry"]
    dim_financial_institution = dimensions["dim_financial_institution"]
    dim_source_money = dimensions["dim_source_money"]
    dim_date = dimensions["dim_date"]

    # =========================================================
    # COUNTRY DIMENSION
    # Dipakai 3 kali:
    # 1. country
    # 2. destination_country
    # 3. tax_haven_country
    # =========================================================

    country_origin = dim_country.alias("country_origin")
    country_destination = dim_country.alias("country_destination")
    country_tax_haven = dim_country.alias("country_tax_haven")

    fact_df = (
        df.alias("t")
        
        # Country asal transaksi
        .join(
            country_origin,
            col("t.country") == col("country_origin.country"),
            "left"
        )

        # Country tujuan
        .join(
            country_destination,
            col("t.destination_country") == col("country_destination.country"),
            "left"
        )

        # Tax haven country
        .join(
            country_tax_haven,
            col("t.tax_haven_country") == col("country_tax_haven.country"),
            "left"
        )

        # Person
        .join(
            dim_person.alias("p"),
            col("t.person_involved") == col("p.person_involved"),
            "left"
        )

        # Transaction Type
        .join(
            dim_transaction_type.alias("tt"),
            col("t.transaction_type") == col("tt.transaction_type"),
            "left"
        )

        # Industry
        .join(
            dim_industry.alias("i"),
            col("t.industry") == col("i.industry"),
            "left"
        )

        # Financial Institution
        .join(
            dim_financial_institution.alias("fi"),
            col("t.financial_institution") == col("fi.financial_institution"),
            "left"
        )

        # Source of Money
        .join(
            dim_source_money.alias("sm"),
            col("t.source_of_money") == col("sm.source_of_money"),
            "left"
        )

        # Date
        .join(
            dim_date.alias("d"),
            to_date(col("t.date_of_transaction")) == col("d.full_date"),
            "left"
        )

        # =====================================================
        # PILIH KOLOM FACT
        # =====================================================

        .select(
            col("t.transaction_id").alias("transaction_id"),

            col("d.date_key").alias("date_key"),

            col("country_origin.country_key").alias("country_key"),

            col("country_destination.country_key")
                .alias("destination_country_key"),

            col("country_tax_haven.country_key")
                .alias("tax_haven_country_key"),

            col("p.person_key").alias("person_key"),

            col("tt.transaction_type_key")
                .alias("transaction_type_key"),

            col("i.industry_key").alias("industry_key"),

            col("fi.financial_institution_key")
                .alias("financial_institution_key"),

            col("sm.source_money_key")
                .alias("source_money_key"),

            col("t.amount_usd").alias("amount_usd"),

            col("t.money_laundering_risk_score")
                .alias("money_laundering_risk_score"),

            col("t.shell_companies_involved")
                .alias("shell_companies_involved"),

            col("t.reported_by_authority")
                .alias("reported_by_authority")
        )
    )

    # Membuat transaction_key sebagai surrogate key fact
    fact_window = Window.orderBy("transaction_id")

    fact_df = (
        fact_df
        .withColumn(
            "transaction_key",
            row_number().over(fact_window)
        )
        .select(
            "transaction_key",
            "transaction_id",
            "date_key",
            "country_key",
            "destination_country_key",
            "tax_haven_country_key",
            "person_key",
            "transaction_type_key",
            "industry_key",
            "financial_institution_key",
            "source_money_key",
            "amount_usd",
            "money_laundering_risk_score",
            "shell_companies_involved",
            "reported_by_authority"
        )
    )

    return fact_df