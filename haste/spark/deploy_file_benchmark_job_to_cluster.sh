#!/usr/bin/env bash

# python3 setup.py bdist_egg

# rsync dist/spark_streaming_benchmark-0.1-py3.5.egg lovisainstance:~/
rsync file_streaming_benchmark.py ben-spark-master:~/

# Deploy mode:
# cluster: run remotely, report back console output
# client: relay everything, run it locally.
# Note: 'cluster' deploy mode not supported for Python: https://spark.apache.org/docs/latest/submitting-applications.html

# LovisaInstance: 192.168.1.13

ssh ben-spark-master 'PYSPARK_PYTHON=python3 \
    SPARK_HOME=/usr/local/spark ; \
    $SPARK_HOME/bin/spark-submit \
    --master spark://ben-spark-master:7077 \
    --deploy-mode client \
    --verbose \
    file_streaming_benchmark.py'

#    --conf spark.streaming.blockInterval=50ms \
#    --supervise \
#    --verbose \



# this works OK, but fails when run directly because of some issue with env vars for which python to use ?!