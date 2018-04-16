0. Setup to get dependencies installed:
 ```
 $ pip3 install -e . 
 ```

# Benchmarking HarmonicIO with the microscope simulator

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

# Sever Based Streaming (for example, with Spark)

```bash
python3 -m haste.benchmarking.streaming_server 
```

Control server will listen on :8080

SEE ALSO: haste/benchmarking/spark/readme.MD




Contributors: Ben Blamey


TODO: move the hosts from HarmonicIOSetup into this repo?