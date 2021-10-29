import pandas as pd

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/") \
    .enableHiveSupport() \
    .getOrCreate()

res = spark.sql("SELECT * FROM userdb.table_a")

df = pd.DataFrame(res.toPandas())
print(df)
print("======================")
