import time
from haste.benchmarking.throttling_app.monitor_spark_driver import SparkMonitor
from sortedcontainers import SortedDict
import threading
import statistics
from .streaming_server_rest_client import set_new_params
from ...spark.file_streaming_benchmark import BATCH_INTERVAL_SECONDS
import calendar
from ..streaming_server.file_streaming import MESSAGE_SIZES

FETCH_STATUS_INTERVAL_SECONDS = 5

# Using port forwarding.
HOST_FOR_SPARK_REST_API = 'localhost'
NUMBER_OF_BATCHES = 10  # Number of batches to wait before computing new frequency

# TODO: hmmm... includes the scheduling delay - what if it simply gets behind. get it errs on caution.
# key[0] is the *oldest* key[-1] is the *newest*
total_delays_secs_by_timestamp = SortedDict()
have_queued_jobs = False

# wait a few batch intervals before the initial increase
frequency_last_set = -1
frequency = -1

monitor = SparkMonitor(HOST_FOR_SPARK_REST_API)
monitor.start()


def fetch_total_delays():
    # while True:

    try:
        stats = monitor.get_status()
    except:
        import sys
        sys.exit()

    # print(stats)

    queued_job_count = 0
    for batch in stats['batches']:
        if batch['status'] != 'COMPLETED':
            if batch['status'] == 'QUEUED':
                queued_job_count += 1

            print('..skipping batch ' + str(batch['batchId']) + ' with status ' + str(batch['status']))
            continue

        timestamp = calendar.timegm(time.strptime(batch['batchTime'], '%Y-%m-%dT%H:%M:%S.000%Z'))
        total_delay = int(batch.get('totalDelay', -1)) / 1000
        if total_delay < 0:
            print('..skipping batch ' + str(batch['batchId']) + ' with missing totalDelay')
            continue

        # TODO: print out the data
        if timestamp not in total_delays_secs_by_timestamp:
            import datetime
            print('fetched batch: ' + datetime.datetime.utcfromtimestamp(timestamp).strftime(
                '%Y-%m-%dT%H:%M:%SZ') + ' ' + str(total_delay) + 's')
            total_delays_secs_by_timestamp[timestamp] = total_delay

        have_queued_jobs = queued_job_count > 1

    # print(total_delays_secs_by_timestamp.items()[-1])

    # TODO: the whole throttling decision depends on this info - might as well put the code in here
    # (idea of separate threads was if getting the data was slow)

    # time.sleep(FETCH_STATUS_INTERVAL_SECONDS)


def find_max_throughput(message_size_bytes, cpu_cost_ms, initial_frequency=1):
    global frequency_last_set, frequency

    frequency = initial_frequency

    set_new_freq(frequency, message_size_bytes=message_size_bytes, cpu_cost_ms=cpu_cost_ms)

    while True:
        time.sleep(1)
        new_frequency = None

        fetch_total_delays()

        # print(total_delays_secs_by_timestamp)

        recent_delays = SortedDict({t: delay for t, delay in total_delays_secs_by_timestamp.items() if
                                    t > frequency_last_set + BATCH_INTERVAL_SECONDS})

        if len(recent_delays) == 0:
            # print('waiting for next batch completion...')
            continue

        print(recent_delays)
        latest_total_delay = recent_delays.values()[-1]
        # print(latest_total_delay)
        if frequency > 1 and latest_total_delay > BATCH_INTERVAL_SECONDS * 5:
            # Spark can't cope
            print('total delay is now: ' + str(latest_total_delay) + ' - spark cant cope - reverting to 1Hz')
            new_frequency = throttle_down(frequency)
        else:
            # latest_total_delays = total_delays_secs_by_timestamp.values()[-min(NUMBER_OF_BATCHES,
            #                                                                    len(total_delays_secs_by_timestamp)):]
            # print(latest_total_delays)
            mean_total_delay = statistics.mean(list(recent_delays.values()))
            print('mean total delay is now: ' + str(mean_total_delay) + ' target frequency is: ' + str(frequency) + ' message size is ' + str(message_size))

            if len(recent_delays) > 5 and mean_total_delay > 2 * BATCH_INTERVAL_SECONDS:
                # throttle down early if we're struggling - but allow time to recover from before
                print('(B) we we overshot! frequency was: ' + str(frequency))
                new_frequency = throttle_down(frequency)
                # FIXME: if the streaming app fails for some reason (out of disk space) - it just increases forever!
                # hack: filter to only look at batches since we last raised the rate - there might not be any yet
            elif len(recent_delays) > NUMBER_OF_BATCHES:
                longest_total_delays = sorted(list(recent_delays.values()))
                total_delay_high = max(longest_total_delays[-2], longest_total_delays[-1], mean_total_delay)
                print('total_delay_high: ' + str(total_delay_high))

                print('waited a few intervals since the last change of frequency, should we increase?..')
                if total_delay_high < BATCH_INTERVAL_SECONDS * 0.01:
                    new_frequency = frequency * 50
                elif total_delay_high < BATCH_INTERVAL_SECONDS * 0.1:
                    new_frequency = frequency * 5
                elif total_delay_high < BATCH_INTERVAL_SECONDS * 0.5:
                    new_frequency = int(frequency * 1.75) + 1
                elif total_delay_high < BATCH_INTERVAL_SECONDS * 0.9:
                    new_frequency = int(frequency * 1.03) + 1
                elif total_delay_high < BATCH_INTERVAL_SECONDS:
                    # we consider this our max stable throughput
                    print('max throughput is:' + str(frequency) + ' message size: ' + message_size_bytes)
                    return
                else:
                    print('we we overshot! frequency was: ' + str(frequency) + ' message size: ' + str(
                        message_size_bytes))
                    new_frequency = throttle_down(frequency)
                    # TODO: elif: we overshot - scale back down!

        if new_frequency is not None and new_frequency != frequency:
            set_new_freq(new_frequency, message_size_bytes=message_size_bytes, cpu_cost_ms=cpu_cost_ms)


def throttle_down(frequency):
    return int(max(1, frequency / 2))


def set_new_freq(new_frequency, message_size_bytes, cpu_cost_ms=20):
    # hack: spark jobs don't start until files are all read,
    # so to prevent frequency escalation - clean

    total_delays_secs_by_timestamp = {}

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


    # fetch_total_delays_thread = threading.Thread(target=fetch_total_delays)
    # fetch_total_delays_thread.start()

    # find_max_throughput_thread = threading.Thread(target=find_max_throughput)
    # find_max_throughput_thread.start()


for message_size in MESSAGE_SIZES:
    if message_size < 100000:
        print('skipping ' + str(message_size))
        continue
    print('message_size: ' + str(message_size))

    find_max_throughput(message_size, 20)


# find_max_throughput(500, 20, initial_frequency=100)

# find_max_throughput(1000, 20, initial_frequency=100)

# find_max_throughput(100000, 20, initial_frequency=10)
