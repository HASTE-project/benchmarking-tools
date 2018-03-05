import datetime
from .harmonic_io import run_remote_ssh, ensure_exactly_containers, \
    NUMBER_WORKER_NODES, DOCKER_IMAGE_URL, ensure_normal_production_state, remove_stopped_containers

SIMULATOR_HOSTNAME = 'lovisainstance'

result_filename = './results/' + datetime.datetime.today().strftime('%Y_%m_%d__%H_%M_%S') + '_benchmarking.txt'
print('output to: ' + result_filename)


def append_result_to_file(str):
    # append a line of text:
    print(str)
    with open(result_filename, "a") as file:
        file.write(str)
        file.write('\n')


def run_simulator():
    # Streams 500 images, then polls MongoDB to wait for completion, and outputs benchmark info as CSV to stdout:
    completed_process = run_remote_ssh('python3 ./exjobb/benchmark_full_pipeline.py', hosts=SIMULATOR_HOSTNAME)
    stdout = completed_process.stdout.decode('utf-8')

    benchmarks = [row.split(',') for row in stdout.splitlines() if row.startswith('benchmarking,')]
    # print(benchmarks)

    # Out:
    # {'benchmarking': 'benchmarking', 'file': 'benchmark_full_pipeline', 'topic': 'full', 'description': '',
    #  'started_at_time': '1517925196.327163', 'ended_time': '1517925292.111895', 'duration_secs': '95.78473210334778',
    #  'number_of_bytes': '-1', 'container_count': 1}

    # 3rd column is the 'tag' -- extract the 'full' result - this is the total duration to run the pipeline.
    benchmark_total = [row for row in benchmarks if row[1] == 'benchmark_full_pipeline' and row[2] == 'full'][0][6]
    prepare_total = [row for row in benchmarks if row[1] == 'simulator_no_flask' and row[2] == 'prepared_to_stream'][0][6]

    # total - prepare = 'real' processing time
    result = {'total': benchmark_total, 'prepare': prepare_total}
    print(result)
    return result


def run_test(container_count):
    print('container count is: {}'.format(container_count))
    ensure_exactly_containers(container_count)

    result = run_simulator()

    result['container_count'] = container_count

    append_result_to_file(str(result))

    # 6th column is the total time:


def benchmarks():
    # (Assuming that deployHIO has been run.)

    # Pull latest image we will use for benchmarking:
    run_remote_ssh('sudo docker pull {}'.format(DOCKER_IMAGE_URL), hosts='workers')

    for container_count in range(1, NUMBER_WORKER_NODES + 1):
        try:
            run_test(container_count)
        except Exception as e:
            print('exception in run_test(..):')
            print(e)
            print('(continuing)')


if __name__ == '__main__':
    # remove containers from last time
    try:
        remove_stopped_containers()
        benchmarks()
    finally:
        ensure_normal_production_state()
    # we don't remove containers here, so that we can debug any errors.
