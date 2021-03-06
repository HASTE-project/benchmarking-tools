#!/usr/bin/env bash

# python3 setup.py bdist_egg

# rsync dist/spark_streaming_benchmark-0.1-py3.5.egg lovisainstance:~/


# Deploy mode:
# cluster: run remotely, report back console output
# client: relay everything, run it locally.
# Note: 'cluster' deploy mode not supported for Python: https://spark.apache.org/docs/latest/submitting-applications.html

# LovisaInstance: 192.168.1.13
#while true; do

#ssh ben-spark-master 'PYSPARK_PYTHON=python3 \
#    SPARK_HOME=/usr/local/spark ; \
#    $SPARK_HOME/bin/spark-submit \
#    --master spark://ben-spark-master:7077 \
#    --conf spark.streaming.fileStream.minRememberDuration=30s \
#    --conf spark.cleaner.periodicGC.interval=10s \
#    --deploy-mode client \
#    file_streaming_benchmark.py'

while true; do
    rsync file_streaming_benchmark.py ben-spark-master:~/
    ssh -t -t ben-spark-master 'PYSPARK_PYTHON=python3 \
        SPARK_HOME=/usr/local/spark ; \
        python3 file_streaming_benchmark.py'
    echo 'restarting soon..'
    # leave long enough so the old app fails (and we get to re-use port 4040)
    sleep 20
done


# minRememberDuration must be x2 batch interval or bigger (https://halfvim.github.io/2016/06/28/FileInputDStream-in-Spark-Streaming/)
# see: FileInputDStream.scala
# otherwise new files might not get picked up.



#    --conf "spark.executor.extraJavaOptions=-verbose:gc \
#    --conf "spark.driver.extraJavaOptions=-verbose:gc \

#    --conf spark.streaming.blockInterval=50ms \
#    --supervise \
#    --verbose \



# this works OK, but fails when run directly because of some issue with env vars for which python to use ?!