# KafkaRowFormat
此项目为云南移动日累计需求：spark streaming按竖线拆分多行。

使用步骤：  
1.idea下载项目，执行mvn clean package编译  
2.将编译好的KafkaRowFormat-1.0.jar、common.properties配置文件、start-app.sh执行脚本一块上传到OCSP所在服务器，放在如下目录：/home/ocdp/KafkaRowFormat下，KafkaRowFormat-1.0.jar放到该目录的lib下面。  
3.修改common.properties配置文件，输入输出topic名称等参数。  
4.在KafkaRowFormat目录下执行sh脚本启动spark-submit命令。  
