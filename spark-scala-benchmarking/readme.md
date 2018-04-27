


4 components:

1. python streaming server - can stream messages either over TCP or to disk
2. PySpark application which processes messages (either over TCP or from disk)
3. Scala Spark application which processes messages (only supports disk)
4. Python-based throttling app - queries running application, and throttles stream source application to determine 
max throughput. 