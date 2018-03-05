#!/usr/bin/env bash

# python3 setup.py bdist_egg

# rsync dist/spark_streaming_benchmark-0.1-py3.5.egg lovisainstance:~/
rsync streaming_benchmark.py lovisainstance:~/

# Deploy mode:
# cluster: run remotely, report back console output
# client: relay everything, run it locally.
# Note: 'cluster' deploy mode not supported for Python: https://spark.apache.org/docs/latest/submitting-applications.html

# LovisaInstance: 192.168.1.13

ssh lovisainstance 'SPARK_HOME=~/spark-2.2.1-bin-hadoop2.7 ; \
    PYSPARK_PYTHON=python3 \
    $SPARK_HOME/bin/spark-submit \
    --master spark://192.168.1.33:7077 \
    --deploy-mode client \
    streaming_benchmark.py'

#    --supervise \
#    --verbose \



# this works OK, but fails when run directly because of some issue with env vars for which python to use ?!