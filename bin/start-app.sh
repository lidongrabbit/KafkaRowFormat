#kinit -kt /home/ocdp/KafkaRowFormat/om-ocdp-kafka.keytab ocdp@ynmobile.com
#!/bin/sh
########启动Spark streaming流任务########
current_date=`date -d "-1 day" "+%Y%m%d"`
echo "当前日期："$current_date

work_home=/home/ocdp/KafkaRowFormat
echo "脚本执行目录："$work_home

nohup sudo -u ocdp /usr/hdp/2.6.0.3-8/spark/bin/spark-submit --files “$work_home/om-ocdp-kafka_jaas.conf,$work_home/om-ocdp-kafka.keytab,$work_home/common.properties” --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./om-ocdp-kafka_jaas.conf" --driver-java-options "-Djava.security.auth.login.config=$work_home/om-ocdp-kafka_jaas.conf" --keytab $work_home/om-ocdp-spark.keytab --principal ocdp@ynmobile.com --class com.asiainfo.ocsp.yunnan.StreamApp --master yarn --deploy-mode client --driver-memory 20g --executor-memory 2g --num-executors 50 --executor-cores 4 --queue ocsp  --jars 包含ocsp执行脚本中所有jar,$work_home/lib/KafkaRowFormat-1.0.jar $work_home/lib/KafkaRowFormat-1.0.jar &> $work_home/log/app_$current_date.log &