export SPARK_HOME=/opt/spark
export HADOOP_USER_NAME=hdfs

build_venv_python3(){
    DIR="$( cd "$( dirname "$0" )" && pwd )"
    python3 -m venv venv
    pip3 install -r ${DIR}/requirements.txt
    python3 -m compileall .
    deactivate
}

build_venv_python2(){
    DIR="$( cd "$( dirname "$0" )" && pwd )"
    virtualenv -p $(which python) venv
    python -m venv venv
    . ${DIR}/venv/bin/activate
    pip install -r ${DIR}/requirements.txt
    python -m compileall .
    deactivate
}

version=$1
case $version in
  '2.7')
    build_venv_python2
    ;;
  '3')
    build_venv_python3
    ;;
  *)
    echo "wrong input version. please give version 2.7 or 3."
    ;;
esac