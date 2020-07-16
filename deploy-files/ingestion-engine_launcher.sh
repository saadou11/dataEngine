#!/bin/bash

$SPARK_HOME/bin/spark-submit \
--master yarn \
--properties-file application.properties \
--jars common-1.0-jar-with-dependencies.jar \
--class MainIngestion \
ingestion-engine-1.0.jar
