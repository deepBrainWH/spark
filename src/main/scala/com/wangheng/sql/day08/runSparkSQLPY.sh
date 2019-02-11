#!/bin/bash
/usr/app/spark/bin/spark-submit \
--jars /home/wangheng/.m2/repository/mysql/mysql-connector-java/8.0.14/mysql-connector-java-8.0.14.jar \
./SparkSQL.py