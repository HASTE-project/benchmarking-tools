#!/usr/bin/env bash

rsync word_count_example.py ben-spark-master:~/

# Deploy mode:
# cluster: run remotely, report back console output
# client: relay everything, run it locally.
# Note: 'cluster' deploy mode not supported for Python: https://spark.apache.org/docs/latest/submitting-applications.html

ssh ben-spark-master 'SPARK_HOME=/usr/local/spark ; \
    PYSPARK_PYTHON=python3 \
    $SPARK_HOME/bin/spark-submit \
    --master spark://ben-spark-master:7077 \
    --conf spark.streaming.blockInterval=50ms \
    --deploy-mode client \
    word_count_example.py'

#    --supervise \
#    --verbose \
