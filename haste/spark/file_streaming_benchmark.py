import time

import os
os.environ['PYSPARK_PYTHON'] = 'python3'  # executors
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'  # driver

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext(appName="FileStreamingBenchmark")

ssc = StreamingContext(sc, 5)  # second argument is the batch interval in seconds.
# for file based streaming from an NFS share - needs to be high because listing the files takes a while


# Self-contained - so that it can be submitted as a single script (no external deps. ex. Spark)

# Port for Streaming Server is 9999
# IP address that worker node will connect to (don't use localhost or 127.0.0.1 in a cluster context)
#lines = ssc.socketTextStream('192.168.1.13', 9999)  # LovisaInstance
#lines = ssc.socketTextStream('192.168.1.51', 9999)  # spark-stream-server
#lines = ssc.socketTextStream('localhost', 9999)

if False:
    lines = ssc.socketTextStream('ben-stream-src', 9999)
else:
    USE_RAMDISK = True
    if USE_RAMDISK:
        lines = ssc.textFileStream('/mnt/nfs/ben-stream-src-3-shm-bench')
    else:
        lines = ssc.textFileStream('/mnt/nfs/ben-stream-src-3/bench2')


# Copied from messaging.py
def parse_message(line):
    return {'cpu_pause_ms': int(line[1:7])}


# Copied from pause.py
def cpu_pause(secs):
    if secs > 0:
        start = time.time()
        while time.time() < start + secs:
            #print('.')
            x = 0
            for n in range(1000):
                x = x + 1
    else:
        pass


def process_line(line):
    #print("line length: %d" % len(line))
    parsed = parse_message(line)
    sleep_ms = parsed['cpu_pause_ms']
    sleep_secs = float(sleep_ms) / 1000
    # print(sleep_secs)
    # Should do some work instead to keep the core busy
    # Spark tracks the cores so think its OK
    cpu_pause(sleep_secs)


lines.map(process_line).count().pprint()

#ssc.remember(20) # release RDDs for garbage collection after 1 second

ssc.start()
ssc.awaitTermination()
