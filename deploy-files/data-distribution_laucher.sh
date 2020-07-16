#!/bin/bash

$SPARK_HOME/bin/spark-submit \
--master yarn \
--class BOOT-INF.classes.applications.ApplicationContextSpring \
data-distribution-1.0.jar
