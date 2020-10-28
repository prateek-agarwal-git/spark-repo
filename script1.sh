#!/bin/bash
$SPARK_HOME/bin/spark-submit  --class "CooccurenceCount" \
  --master local[5] \
  target/CooccurenceCount-1.0.jar
