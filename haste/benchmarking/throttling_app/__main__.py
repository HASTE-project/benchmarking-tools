import time
from haste.benchmarking.throttling_app.monitor_spark_driver import SparkMonitor
from sortedcontainers import SortedDict
import threading
import statistics
from .streaming_server_rest_client import set_new_params

FETCH_STATUS_INTERVAL_SECONDS = 5

HOST = 'localhost'

monitor = SparkMonitor(HOST)

monitor.start()

# TODO: hmmm... includes the scheduling delay - what if it simply gets behind. get it errs on caution.
# key[0] is the *oldest* key[-1] is the *newest*
total_delays_secs_by_timestamp = SortedDict()
total_delays_secs_by_timestamp_lock = threading.Lock()


def fetch_total_delays():
    while True:

        try:
            stats = monitor.get_status()
        except:
            continue

        # print(stats)

        with total_delays_secs_by_timestamp_lock:
            for batch in stats['batches']:
                if batch['status'] != 'COMPLETED':
                    print('..skipping batch ' + str(batch['batchId']) + ' with status ' + str(batch['status']))
                    continue

                timestamp = batch['batchTime']
                total_delay = int(batch.get('totalDelay', -1)) / 1000
                if total_delay < 0:
                    print('..skipping batch ' + str(batch['batchId']) + ' with missing totalDelay')
                    continue

                # TODO: print out the data
                total_delays_secs_by_timestamp[timestamp] = total_delay

        # print(total_delays_secs_by_timestamp.items()[-1])

        # TODO: the whole throttling decision depends on this info - might as well put the code in here
        # (idea of separate threads was if getting the data was slow)

        time.sleep(FETCH_STATUS_INTERVAL_SECONDS)


BATCH_INTERVAL_SECONDS = 10
NUMBER_OF_BATCHES = 5  # Number of batches to wait before computing new frequency

# wait a few batch intervals before the initial increase
frequency_last_set = time.time() + (BATCH_INTERVAL_SECONDS * 2)

frequency = -1


def find_max_throughput():
    global frequency_last_set, frequency

    message_size_bytes = 500000
    cpu_cost_ms = 20

    set_new_freq(1, message_size_bytes=message_size_bytes, cpu_cost_ms=cpu_cost_ms)

    while True:
        time.sleep(1)
        new_frequency = None

        if len(total_delays_secs_by_timestamp) == 0:
            print('waiting for first batch completion...')
            continue

        latest_total_delay = total_delays_secs_by_timestamp.values()[-1]
        print(latest_total_delay)

        if frequency is not 1 and latest_total_delay > BATCH_INTERVAL_SECONDS * 1.3:
            # Spark can't cope
            print('total delay is now: ' + str(latest_total_delay) + ' - spark cant cope - reverting to 1Hz')
            new_frequency = 1
        else:
            latest_total_delays = total_delays_secs_by_timestamp.values()[-min(NUMBER_OF_BATCHES,
                                                                               len(total_delays_secs_by_timestamp)):]
            # print(latest_total_delays)
            mean_total_delay = statistics.mean(latest_total_delays)
            print('mean total delay is now: ' + str(mean_total_delay) + ' target frequency is: ' + str(frequency))

            # FIXME: if the streaming app fails for some reason (out of disk space) - it just increases forever!

            if time.time() > ((NUMBER_OF_BATCHES + 1) * BATCH_INTERVAL_SECONDS) + frequency_last_set:
                print('waited a few intervals since the last change of frequency, should we increase?..')
                if mean_total_delay < BATCH_INTERVAL_SECONDS * 0.01:
                    new_frequency = frequency * 50
                elif mean_total_delay < BATCH_INTERVAL_SECONDS * 0.1:
                    new_frequency = frequency * 5
                elif mean_total_delay < BATCH_INTERVAL_SECONDS * 0.5:
                    new_frequency = frequency * 2
                elif mean_total_delay < BATCH_INTERVAL_SECONDS * 0.9:
                    new_frequency = int(frequency * 1.1)
                elif mean_total_delay < BATCH_INTERVAL_SECONDS:
                    # we consider this our max stable throughput
                    print('max throughput is:' + str(frequency))
                    exit(0)
                # TODO: elif: we overshot - scale back down!

        if new_frequency is not None and new_frequency != frequency:
            set_new_freq(new_frequency, message_size_bytes=message_size_bytes, cpu_cost_ms=cpu_cost_ms)


def set_new_freq(new_frequency, message_size_bytes=500000, cpu_cost_ms=20):
    global frequency, frequency_last_set
    frequency = new_frequency
    frequency_last_set = time.time()
    print('setting new frequency...' + str(frequency))
    new_params = {
        "cpu_pause_ms": cpu_cost_ms,
        "message_bytes": message_size_bytes,
        "period_sec": float(1 / frequency)
    }
    set_new_params(new_params)


fetch_total_delays_thread = threading.Thread(target=fetch_total_delays)
fetch_total_delays_thread.start()

find_max_throughput_thread = threading.Thread(target=find_max_throughput)
find_max_throughput_thread.start()
