from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.appName("BronzeIngestion")
        .config(
            "spark.jars",
            "/opt/spark/work-dir/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/jars/hadoop-aws-3.3.4.jar",
        )
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

    input_dir = "/opt/spark/data"
    output_bucket = "s3a://bronze"

    tables = [
        "customers",
        "customer_history",
        "accounts",
        "account_customer_bridge",
        "branches",
        "dim_date",
        "transactions",
        "account_balance_snapshot",
        "loan_lifecycle",
    ]

    for table in tables:
        print(f"[{table}] cədvəli oxunur...")
        csv_path = f"{input_dir}/{table}.csv"

        df = spark.read.csv(csv_path, header=True, inferSchema=True)

        output_path = f"{output_bucket}/{table}"

        if table == "transactions":
            df.write.mode("overwrite").partitionBy("date_key").parquet(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)

        print(f"[{table}] uğurla Bronze qatına (Parquet olaraq) yazıldı!")

    spark.stop()


if __name__ == "__main__":
    main()
