#!/usr/bin/env bash


# rsync dist/spark_streaming_benchmark-0.1-py3.5.egg lovisainstance:~/
rsync target/scala-2.11/spark-scala-benchmarking_2.11-0.1.jar ben-spark-master:~/

# Deploy mode:
# cluster: run remotely, report back console output
# client: relay everything, run it locally.


ssh ben-spark-master -t -t 'SPARK_HOME=/usr/local/spark-2.3.0-bin-hadoop2.7 ; \
    PYSPARK_PYTHON=python3 \
    $SPARK_HOME/bin/spark-submit \
    --master spark://ben-spark-master:6066 \
    --deploy-mode client \
    --class "FileStreamingBenchmark" \
    spark-scala-benchmarking_2.11-0.1.jar' | tee ../spark-app.log

#    --supervise \
#    --verbose \
#    --conf spark.streaming.blockInterval=50ms \

