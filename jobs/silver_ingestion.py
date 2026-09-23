from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, to_date


def main():
    spark = (
        SparkSession.builder.appName("SilverIngestion")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    tables = [
        "customers",
        "customer_history",
        "accounts",
        "account_customer_bridge",
        "branches",
        "dim_date",
        "account_balance_snapshot",
        "loan_lifecycle",
    ]

    for table in tables:
        df = spark.read.parquet(f"s3a://bronze/{table}")

        if table == "customers":
            df = df.withColumn(
                "date_of_birth", to_date(col("date_of_birth"), "yyyy-MM-dd")
            )

        df.write.mode("overwrite").parquet(f"s3a://silver/{table}")
        print(f"[{table}] Silver qatına uğurla yazıldı.")

    print("Transactions cədvəli 5 xətadan təmizlənir...")

    df_tx = spark.read.parquet("s3a://bronze/transactions")
    df_accounts = spark.read.parquet("s3a://silver/accounts")

    df_tx = df_tx.withColumn("amount", col("amount").cast("double")).withColumn(
        "is_flagged_fraud", col("is_flagged_fraud").cast("boolean")
    )

    df_tx = df_tx.dropDuplicates(["transaction_id"])

    df_tx = df_tx.dropna(subset=["amount"])

    df_tx = df_tx.withColumn("amount", abs(col("amount")))

    df_tx = df_tx.filter(col("currency") != "XXX")

    valid_account_ids = df_accounts.select("account_id").distinct()
    df_tx_cleaned = df_tx.join(valid_account_ids, on="account_id", how="inner")

    df_tx_cleaned.write.mode("overwrite").partitionBy("date_key").parquet(
        "s3a://silver/transactions"
    )
    print("[transactions] cədvəli təmizləndi və Silver-ə yazıldı")

    spark.stop()


if __name__ == "__main__":
    main()
