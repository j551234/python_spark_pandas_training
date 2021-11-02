#!/bin/bash

DIR="$( cd "$( dirname "$0" )" && pwd )"

export HADOOP_USER_NAME=root
export SPARK_HOME=/opt/spark

. ${DIR}/venv/bin/activate

$SPARK_HOME/bin/spark-submit --master yarn \
 --deploy-mode client \
 submit_job.py

deactivate