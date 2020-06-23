#!/bin/bash

$SPARK_HOME/bin/spark-submit \
--master yarn \
--executor-memory 512m \
--driver-memory 512m \
--properties-file application.properties \
--jars common-1.0-jar-with-dependencies.jar \
--class Main \
eventProducer-1.0.jar
