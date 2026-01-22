from pyspark.sql import SparkSession
# sql读取数据正确性
def create_spark_session():
    return SparkSession.builder \
        .appName("read-clickhouse-backup-task") \
        .getOrCreate()

if __name__ == "__main__":
    spark = create_spark_session()

    # 读取原始备份数据
    path_array = ["oss://wangmeng-database-backup/test_clickhouse_to_oss"]
    us_day_df = spark.read \
        .option("compression", "gzip") \
        .option("recursiveFileLookup", "true") \
        .json(path_array)

    # 注册为临时视图供 SQL 使用
    us_day_df.createOrReplaceTempView("backup_data")

    # 👇 请将 `event_time` 替换为你实际的时间字段名！
    time_field = "create_time"  # ←←← 关键：改成你的真实字段！

    # 使用 SQL 添加 month 分区列（格式 yyyy-MM）
    monthly_df = spark.sql(f"""
        SELECT sum(click_count) AS clicks,
               date_format(to_date({time_field}), 'yyyy-MM') AS month
        FROM backup_data
        GROUP BY date_format(to_date({time_field}), 'yyyy-MM')
    """)

    # 写入结果（自动按 month 分区）
    result_dir = "oss://wangmeng-database-backup/dzk/test_clickhouse_monthly_data"
    monthly_df.write \
        .mode("overwrite") \
        .partitionBy("month") \
        .json(result_dir)

    spark.stop()