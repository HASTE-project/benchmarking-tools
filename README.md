Various utilities for benchmarking HarmonicIO, Apache Spark, and the HASTE pipeline:

0. Python tool for benchmarking HASTE pipeline using simulator
1. python streaming server - can stream messages either over TCP or to disk
2. PySpark application which processes messages (either over TCP or from disk) (deprecated for file streaming)
3. Scala Spark application which processes messages (only supports disk)
4. Python-based throttling app - queries running application, and throttles stream source application to determine 
max throughput. 


# HASTE Benchmarking Tool

0. Setup to get dependencies installed:
 ```
 $ pip3 install -e . 
 ```

## Benchmarking HarmonicIO with the microscope simulator

2. Clone https://github.com/HASTE-project/HarmonicIOSetup
(into a sibling directory)

3. Use it to setup HarmonicIO cluster

4. Set relative path of 'HarmonicIOSetup', number of nodes, etc.

5. To do the benchmarking (saves results as a text file):
```
$ python3 -m haste.benchmarking.benchmark
```

6. To plot results
```
$ python3 -m haste.benchmarking.plot_benchmark
```

7. To pull image and start containers (for production state):
```
python3 -m haste.benchmarking.harmonic_io
``` 

# Python Streaming Server
Deploy on a 'stream-src' host.

Preliminary for disk-based streaming - configure NFS share and mount on all Spark nodes.

```bash
python3 -m haste.benchmarking.streaming_server 
```

Control server will listen on :8080

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



Contributors: Ben Blamey