#!/bin/bash
########启动Spark streaming流任务########
current_date=`date -d "-1 day" "+%Y%m%d"`
echo "当前日期："$current_date

work_home=/home/ocdp/ocsp_rowFormat
echo "脚本执行目录："$work_home

function getJars(){
res=""
filelist=`ls ${work_home}/lib/*.jar`
for file in $filelist
do
        if [ -n "$res" ];then
                res="${res},${work_home}/lib/${file}"
        else
                res="${work_home}/lib/${file}"
        fi
done
echo $res
}

TASK_ID=1
CONF_HOME="${work_home}/conf"
LOG_PATH="${work_home}/logs"
yarn_princ="ocdp/ocdp@ynmobile.com"
driver_mem="2g"
executor_mem="2g"
num_executors=3
executor_cores=10
queue="default"
jars=`getJars`

#echo "jars: " $jars

cd $CONF_HOME
files="$CONF_HOME/executor-log4j.properties,$CONF_HOME/kafka_jaas.conf,$CONF_HOME/kafka.keytab,$CONF_HOME/common.properties"

nohup /usr/hdp/2.6.0.3-8/spark/bin/spark-submit \
--files ${files} \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=executor-log4j.properties -Djava.security.auth.login.config=./kafka_jaas.conf"  \
--driver-java-options "-DOCSP_LOG_PATH=${LOG_PATH} -DOCSP_TASK_ID=${TASK_ID} -Djava.security.auth.login.config=${CONF_HOME}/kafka_jaas.conf -Dlog4j.configuration=file:${CONF_HOME}/driver-log4j.properties" \
--keytab $CONF_HOME/spark.keytab --principal $yarn_princ  \
--class com.asiainfo.ocsp.yunnan.StreamApp \
--master yarn --deploy-mode client --driver-memory ${driver_mem} --executor-memory ${executor_mem} --num-executors ${num_executors} --executor-cores ${executor_cores} \
--queue ${queue} \
--jars $jars $work_home/lib/KafkaRowFormat-1.0.jar ${TASK_ID} > $work_home/logs/app_${TASK_ID}_$current_date.log 2>&1 &
