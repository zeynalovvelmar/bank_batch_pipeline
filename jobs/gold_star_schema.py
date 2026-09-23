from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id


def main():
    spark = (
        SparkSession.builder.appName("GoldStarSchema")
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

    dim_date = spark.read.parquet("s3a://silver/dim_date")
    dim_date.write.mode("overwrite").parquet("s3a://gold/dim_date")
    print("[dim_date] hazırlandı.")

    dim_branch = spark.read.parquet("s3a://silver/branches")
    dim_branch = dim_branch.withColumn("branch_sk", monotonically_increasing_id())
    dim_branch.write.mode("overwrite").parquet("s3a://gold/dim_branch")
    print("[dim_branch] hazırlandı.")

    customers = spark.read.parquet("s3a://silver/customers").drop(
        "load_run_id", "source_system", "processed_at"
    )
    customer_history = spark.read.parquet("s3a://silver/customer_history")

    dim_customer = customer_history.join(customers, on="customer_id", how="inner")
    dim_customer = dim_customer.withColumn("customer_sk", monotonically_increasing_id())
    dim_customer.write.mode("overwrite").parquet("s3a://gold/dim_customer")
    print("[dim_customer] hazırlandı (SCD Type 2).")

    dim_account = spark.read.parquet("s3a://silver/accounts")
    dim_account = dim_account.withColumn("account_sk", monotonically_increasing_id())
    dim_account.write.mode("overwrite").parquet("s3a://gold/dim_account")
    print("[dim_account] hazırlandı.")

    transactions = spark.read.parquet("s3a://silver/transactions")

    fact_tx = (
        transactions.alias("t")
        .join(
            dim_account.alias("a"), col("t.account_id") == col("a.account_id"), "inner"
        )
        .join(dim_branch.alias("b"), col("a.branch_id") == col("b.branch_id"), "inner")
        .join(dim_date.alias("d"), col("t.date_key") == col("d.date_key"), "inner")
        .join(
            dim_customer.alias("c"),
            (col("a.customer_id") == col("c.customer_id"))
            & (col("d.full_date") >= col("c.effective_from"))
            & (
                (col("d.full_date") <= col("c.effective_to"))
                | col("c.effective_to").isNull()
            ),
            "inner",
        )
        .select(
            col("t.transaction_id"),
            col("a.account_sk"),
            col("b.branch_sk"),
            col("c.customer_sk"),
            col("t.date_key"),
            col("t.amount"),
            col("t.currency"),
            col("t.channel"),
            col("t.status"),
            col("t.is_flagged_fraud"),
            col("t.transaction_ref"),
        )
    )

    fact_tx.write.mode("overwrite").partitionBy("date_key").parquet(
        "s3a://gold/fact_transaction"
    )
    print("[fact_transaction] hazırlandı və Gold qatına yazıldı!")

    spark.stop()


if __name__ == "__main__":
    main()
