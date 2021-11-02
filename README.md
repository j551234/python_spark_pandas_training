# python_training

使用 kaggle dataset
https://www.kaggle.com/datasnaek/youtube-new

* 轉換出列出所有影片的類別
* 列出 所有 rap 影片 的dislike 數目
* 找出發布時間與地區 和 喜歡人數的線性關係  (draw graph)
* 排序出最多人不喜歡的前十名種類影片
* 排序出前十名的類別 喜歡 和 不喜歡的比例 擁有最大的變異數

*  hellobook.ipynb 基本python 操作
*  use_pandas.ipynb 使用 pandas 來操作dataframe
*  use_spark.ipynb 使用pyspark來操作dataframe

## usage 
*  pip install -r requirements.txt
*  
*  在run on yarn 需給予hadoop的連線資訊，給予HADOOP_CONF_DIR的位置os.environ["HADOOP_CONF_DIR"] = "/opt/hadoop/etc/hadoop"
*  在run on spark 則需指定 spark的位置，"spark://master:7077"
*
## connect folder
* 此為連線mysql跟hive 範例
* 連線需給予jdbc的driver的位置，若是連線hive則要給將hive-site.xml放在資料夾下


## submit folder
* 裡面為使用spark shell submit
* 使用式./build.sh 建立環境跟需要的library
* 跟執行./submit-jobs.sh 
* 使用hive，需把jdbc driver 放在 spark 的jars資料夾下
* 連線需給予jdbc的driver的位置，若是連線hive則要給將hive-site.xml放在資料夾下

 
