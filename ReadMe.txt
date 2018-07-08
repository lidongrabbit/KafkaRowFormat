# KafkaRowFormat
此工程主要是Kafka Spark Streaming定制化开发需求
yunnan目录下为云南拆分竖线需求
shanghai目录下为上海批次去重需求：同一批次消费的kafka数据中，相同IMSI、LAC、CI的信令只保留一条，
即：根据key(IMSI、LAC、CI)去重，内存中相同key的只保留一条，最终输出到kafka中。

使用步骤：  
1.idea下载项目，执行mvn clean package编译  
2.将编译好的tar包拷贝到服务器，例如目录：/home/ocdp/下并解压。
3.修改conf目录下common.properties配置文件，输入、输出topic名称等参数。
4.修改bin目录下start-app.sh中目录、executor等参数，然后执行sh脚本启动spark-submit命令。
