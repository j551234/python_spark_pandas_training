from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]") \
                    .appName('py_spark') \
                    .getOrCreate()

local_path ='/home/james/python_practice/archive'


# 載入檔案
df = spark.read.option("multiline","true").json(local_path + "/" + "CA_category_id.json")

category_df = df.select('items').toJSON().collect()

