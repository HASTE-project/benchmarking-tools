from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Based on:
# https://github.com/apache/spark/blob/master/examples/src/main/python/streaming/network_wordcount.py

# This example uses the older Streaming API

# $ nc -lk 9999

spark_context = SparkContext(appName="PythonStreamingNetworkWordCount")

BATCH_INTERVAL_SECONDS = 5

spark_streaming_context = StreamingContext(spark_context, BATCH_INTERVAL_SECONDS)

lines = spark_streaming_context.socketTextStream('ben-stream-src-3', 9999)

lines.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b).pprint()

spark_streaming_context.start()

spark_streaming_context.awaitTermination()
