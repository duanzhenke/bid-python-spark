from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder .appName("read-clickLog-task") .getOrCreate()


if __name__ == "__main__":
    spark = create_spark_session()

    path_array = [
        "oss://bid-api-track-log02/trackLog/2025/04/03/01/"
    ]

    us_day_df = spark.read \
        .option("compression", "gzip") \
        .option("recursiveFileLookup", "true") \
        .json(path_array) \
        .filter("affiliate_id='10110008'") \
        .filter("offer_id='94807'")

    print("打印 us_day_df.printSchema()")
    us_day_df.printSchema()
    print("打印 us_day_df.printSchema()")

    us_day_df.write \
        .mode("overwrite") \
        .json("oss://wangmeng-database-backup/dzk/offer-94807")

    spark.stop()