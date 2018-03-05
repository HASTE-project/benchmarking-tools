import time
from haste.benchmarking.throttling_app.monitor_spark_driver import SparkMonitor

HOST = 'localhost'

monitor = SparkMonitor(HOST)


while True:
    stats = monitor.get_status()
    avgTotalDelay = stats['avgTotalDelay']

    percent_of_batch_interval = int((avgTotalDelay / 1000) * 100)



    time.sleep(1)