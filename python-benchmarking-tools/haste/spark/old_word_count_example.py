import socket
import threading
import os
import sys

# This example uses the older Streaming API

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext, StreamingListener
from pprint import pprint
from py4j.java_gateway import JavaGateway



class DebugStreamingListener(StreamingListener):

    def onReceiverStarted(self, receiverStarted):
        """
        Called when a receiver has been started
        """
        print('receiver started')
        pprint(receiverStarted)

        # This doesn't work:
        # help = javaGateway.help(receiverStarted, display=False)
        javaGateway = JavaGateway(auto_field=True)
        receiverInfo = javaGateway.get_field(receiverStarted, 'receiverInfo')
        streamId = javaGateway.get_field(receiverInfo, 'streamId')
        print(streamId)
        print(help)


    def onReceiverError(self, receiverError):
        """
        Called when a receiver has reported an error
        """
        print('receiver error')
        pprint(receiverError)

    def onReceiverStopped(self, receiverStopped):
        """
        Called when a receiver has been stopped
        """
        print('receiver stopped')
        pprint(receiverStopped)

    def onBatchSubmitted(self, batchSubmitted):
        """
        Called when a batch of jobs has been submitted for processing.
        """
        print('batch submitted')
        pprint(batchSubmitted)

    def onBatchStarted(self, batchStarted):
        """
        Called when processing of a batch of jobs has started.
        """
        print('batch started')
        pprint(batchStarted)

    def onBatchCompleted(self, batchCompleted):
        """
        Called when processing of a batch of jobs has completed.
        """
        print('batch completed')
        pprint(batchCompleted)

    def onOutputOperationStarted(self, outputOperationStarted):
        """
        Called when processing of a job of a batch has started.
        """
        print('output operation started')
        pprint(outputOperationStarted)

    def onOutputOperationCompleted(self, outputOperationCompleted):
        """
        Called when processing of a job of a batch has completed
        """
        print('output operation completed')
        pprint(outputOperationCompleted)

# conf = (SparkConf()
#          .setMaster("spark://192.168.1.33:7077")
#          .setAppName("PythonStreamingExample"))

# sc = SparkContext(conf = conf)


sc = SparkContext(appName="PythonStreamingNetworkWordCount2")
ssc = StreamingContext(sc, 5)  # second argument is the batch interval in seconds.

# 9999 1MB text file
# 9998 tiny text file
# 9997 nc with stdin

# IP address that worker node will connect to (don't use localhost or 127.0.0.1)
lines = ssc.socketTextStream('192.168.1.33', 9999)
# lines = ssc.socketTextStream('localhost', 9999)

lines.flatMap(lambda line: line.split(" "))\
              .map(lambda word: (word, 1))\
              .reduceByKey(lambda a, b: a+b).pprint()


streamingListener = DebugStreamingListener()

ssc.addStreamingListener(streamingListener=streamingListener)

ssc.start()
ssc.awaitTermination()