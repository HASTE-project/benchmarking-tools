import time
from haste.benchmarking.throttling_app.monitor_spark_driver import SparkMonitor
from sortedcontainers import SortedDict


HOST = 'localhost'

monitor = SparkMonitor(HOST)

monitor.start()


# TODO: hmmm... includes the scheduling delay - what if it simply gets behind. get it errs on caution.
# key[0] is the *oldest* key[-1] is the *newest*
total_delays_by_timestamp = SortedDict()

while True:


    stats = monitor.get_status()

    print(stats)

    for batch in stats['batches']:
        if batch['status'] != 'COMPLETED':
            print('..skipping batch ' + batch['batchId'] + ' with status ' + batch['status'])

        timestamp = batch['batchTime']
        total_delay = batch['totalDelay']

        total_delays_by_timestamp[timestamp] = total_delay



    print(total_delays_by_timestamp.keys()[-1])

    # avgTotalDelay = stats['avgTotalDelay']
    #
    # percent_of_batch_interval = int((avgTotalDelay / 1000) * 100)


    time.sleep(5)