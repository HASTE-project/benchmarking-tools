0. Setup to get dependencies installed:
 ```
 $ pip3 install -e . 
 ```

2. Clone https://github.com/HASTE-project/HarmonicIOSetup
(into a sibling directory)

3. Use it to setup HarmonicIO cluster

4. Set relative path of 'HarmonicIOSetup', number of nodes, etc.

5. To do the benchmarking (saves results as a text file):
```
$ python3 -m benchmarking.benchmark
```

6. To plot results
```
$ python3 -m benchmarking.plot_benchmark
```

Contributors: Ben Blamey

SEE ALSO: haste/benchmarking/spark/readme.MD


TODO: move the hosts from HarmonicIOSetup into this repo?