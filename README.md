# bid-python-spark
python操作spark

<!-- 执行spark-submit命令 -->
spark-submit \
--master yarn \
--driver-cores 1 --driver-memory 1G --executor-cores 2 --executor-memory 5G --num-executors 15 \
oss://wangmeng-database-backup/getClickhosueMonthData.py