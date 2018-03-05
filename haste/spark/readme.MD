## Streaming Server (haste.benchmarking.streaming_server)

Dummy Server for Benchmarking Streaming Applications.
Streams lines of text over TCP, and listens for HTTP POSTs to vary parameters.

Start the server:
```
python3 -m haste.benchmarking.streaming_server
```

Connect (and pipe to /dev/null):
```
sudo sh ./pipe_9999_to_dev_null
```

Vary the params:
```
curl -X POST "localhost:8008/" --data '{"cpu_pause_ms": 20, "message_bytes": 200,"period_sec": 1.0}'
```

## Spark Example App

To run:

0. Download Apache Spark and unzip on local machine.

1. Setup port forwarding to Spark Master Host (e.g.):
```
ssh -L 8080:localhost:8080 -L 8888:localhost:8888 -L 7077:192.168.1.33:7077 spark-prod
```

1. Setup port forwarding to machine where driver application will run (lovisainstance):
```
ssh -L 4040:192.168.1.13:4040 lovisainstance 
```

2. Start netcat (for stdin):
```
nc -lk -p 9999
```

2. Deploy:
```
./pi_example.sh
```

3. Look at Job status (on driver instance):
```
http://localhost:4040
```

3. Look at master status (on master instance):
```
http://localhost:8080
```

4. Upload and deploy the app:
```
./run_job.sh
```

5. Then test by writing some input into `netcat` on the master node. 

6. Kill the job in the master web GUI
7. Shell script should then terminate.

To Pi run example:
```
./bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://192.168.1.33:7077 ~/spark-2.2.1-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.2.1.jar 1000
```
