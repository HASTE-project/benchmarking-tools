


4 components:

1. python streaming server - can stream messages either over TCP or to disk
2. PySpark application which processes messages (either over TCP or from disk)
3. Scala Spark application which processes messages (only supports disk)
4. Python-based throttling app - queries running application, and throttles stream source application to determine 
max throughput. 

# Python Streaming Server
Deploy on a 'stream-src' host.

Preliminary for disk-based streaming - configure NFS share and mount on all Spark nodes.

# PySpark Benchmark Application
Deploy onto driver node.
Works OK for TCP streaming. Issue when files are deleted on disk based streaming (use Scala app instead)

# Scala Spark Application

# Throttling App
Run this locally.

# Deployment Sequence
1. Deploy the Benchmarking Spark App remotely
2. Deploy the Streaming Source App remotely
3. Start the throttling app locally.
