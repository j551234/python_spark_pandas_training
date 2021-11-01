import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as f

os.environ["HADOOP_CONF_DIR"] = "/opt/hadoop/etc/hadoop"


def get_category_file_name(folder_path):
    category_file_list = []
    folder_content = os.listdir(folder_path)
    for item in folder_content:
        if (item.endswith("_category_id.json")):
            category_file_list.append(item)
    return category_file_list


def get_video_file_list(folder_path):
    video_file_list = []
    folder_content = os.listdir(folder_path)
    for item in folder_content:
        if (item.endswith("videos.csv")):
            video_file_list.append(item)
    return video_file_list


def get_category_df(file_name):
    df = spark.read.option("multiline", "true").json(file_name)
    items_df = df.rdd.flatMap(lambda x: x["items"]).toDF()
    title_df = items_df.select('id', items_df.snippet.title.alias("category_name"))
    return title_df


def get_video_df(file_name):
    data_df = spark.read.option("header", True).csv(file_name)
    area = file_name[file_name.index("archive/") + 8:file_name.index("videos.csv")]
    data_df = data_df.withColumn("area", lit(area))
    return data_df


test_path = "/opt/archive"

category_list = get_category_file_name(test_path)
video_list = get_video_file_list(test_path)

spark = SparkSession.builder.master("yarn") \
    .appName('spark-yarn') \
    .getOrCreate()

## 抓出所有影片和分類
test_path = "/opt/archive/"

csvDF = get_video_df(test_path + video_list[0])

for video_item in video_list[1:]:
    csvDF = csvDF.union(get_video_df(test_path + video_item))

jsonDF = get_category_df(test_path + category_list[0])
for category_item in category_list[1:]:
    jsonDF = jsonDF.union(get_category_df(test_path + category_item))

jsonDF = jsonDF.dropDuplicates()
csvDF = csvDF.withColumn("category_id", col('category_id').cast(IntegerType()))

jsonDF = jsonDF.withColumn("id", col('id').cast(IntegerType()))
joinDF = csvDF.join(jsonDF, csvDF['category_id'] == jsonDF['id'])

joinDF.show()

## 列出 所有 rap 影片 的dislike 數目
rap_df = joinDF.filter(joinDF.tags.like("%rap%"))
rap_df[['title', 'dislikes']].show()

## 找出發布時間與地區 和 喜歡人數的線性關係
relation_df = joinDF
relation_df = relation_df.withColumn("likes", col('likes').cast(IntegerType()))
relation_df = relation_df.withColumn("publish_time", hour(relation_df["publish_time"]))
relation_df = relation_df.groupBy("area", "publish_time").sum("likes")
relation_df = relation_df.orderBy(col("area"), col("publish_time"))

pd_df = relation_df.toPandas()

area_array = pd_df['area'].unique()

display(area_array)

for i in area_array:
    pd_each = pd_df[pd_df['area'] == i]
    area = i
    pd_each.plot(x='publish_time', y='sum(likes)', label=area)

## 排序出最多人不喜歡的前十名種類影片
dislike_df = joinDF
dislike_df = dislike_df.groupBy('title').agg(f.sum('dislikes').alias('dislikes')).sort(f.col('dislikes').desc()).head(
    10)
spark.createDataFrame(dislike_df).show()

## 排序出前十名的類別 喜歡 和 不喜歡的比例 擁有最大的變異數
var_df = joinDF
var_df = var_df.withColumn("ratio", f.col("likes") / (f.col("likes") + f.col("dislikes")))

var_df = var_df.groupby('category_name') \
    .agg(f.variance("ratio").alias("ratio")) \
    .sort(f.col('ratio').desc())

display(var_df.head(10))

spark.stop()
