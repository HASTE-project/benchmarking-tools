from subprocess import run, PIPE
import socket
import datetime
import time

# Path to https://github.com/HASTE-project/HarmonicIOSetup
SIMULATOR_HOSTNAME = 'lovisainstance'
HARMONIC_IO_SETUP_PATH = '../HarmonicIOSetup/{}'
NUMBER_WORKER_NODES = 6
DOCKER_IMAGE_URL = 'benblamey/haste-image-proc:latest'
# TODO: factor out port numbers


result_filename = 'results/' + datetime.datetime.today().strftime('%Y_%m_%d__%H_%M_%S') + '_benchmarking.txt'
print('output to: ' + result_filename)


def worker_hostname(i):
    # i start with 1
    return 'hio-worker-prod-{}'.format(i)


def worker_ip_address(i):
    # works because I have setup my /etc/hosts file with the private IPs.
    return socket.gethostbyname(worker_hostname(i))


def worker_ansible_name(i):
    # start with 1
    return 'workernode{}'.format(i)


# TODO: use this below everywhere
def to_harmonic_io_setup_path(filename):
    return HARMONIC_IO_SETUP_PATH.format(filename)


# run_remote_ssh ?
def call_ansible(cmd_str, hosts='workers:master'):
    """
    :param cmd_str: command to run, double-quotes must be double-escaped: \\\"
    :param hosts:
    :return:
    """
    # shell = True means we don't have to specify array of strings for arguments
    proc_result = run('ansible -i ../HarmonicIOSetup/hosts {} -a "{}"'.format(hosts, cmd_str),
                      shell=True, stdout=PIPE, stderr=PIPE)
    print(proc_result.stdout.decode('utf-8'))
    print(proc_result.stderr.decode('utf-8'))
    return proc_result


# run_playbook ?
def call_ansible_playbook(playbook_filename):
    # shell = True means we don't have to specify array of strings for arguments
    proc_result = run(
        'ansible-playbook -i ../HarmonicIOSetup/hosts ../HarmonicIOSetup/playbooks/{}'.format(playbook_filename),
        shell=True, stdout=PIPE, stderr=PIPE)
    print(proc_result.stdout.decode('utf-8'))
    print(proc_result.stderr.decode('utf-8'))
    return proc_result

def stop_all_containers_and_restart_hio():
    # this will clear HIO queues, metadata, etc.

    # stop the HIO master and worker (by quiting screen):
    call_ansible_playbook('stopMasterWorker.yml')

    # stop any running containers: (--quiet = show only numeric IDs)
    # send SIGTERM, after grace period, SIGKILL
    call_ansible('sudo docker stop $(sudo docker ps --all --quiet --filter=\'name={}\')'.format(DOCKER_IMAGE_URL))

    # Start HIO master and workers again
    call_ansible_playbook('startMasterWorker.yml')

    # check they are running (by checking for listening ports)
    # TODO: capture output string and check it is working.
    call_ansible('sh -c \'netstat --numeric --listening --tcp | grep --line-buffered --extended \\\"(8888|8080)\\\"\'')


def start_container_on_node(i):
    ansible_name = worker_ansible_name(i)
    # note curly braces for JSON are doubled to escape them.
    # note that HIO worker does not bind to localhost, we need to use the actual IP address.
    call_ansible('curl -X POST \\\"http://{}:8888/docker?token=None&command=create\\\" --data \'{{\\\"c_name\\\" : \\\"{}\\\", \\\"num\\\" : 1}}\''
                 .format(worker_ip_address(i), DOCKER_IMAGE_URL), hosts=ansible_name)

def start_containers(count):
    for i in range(1, count + 1):
        start_container_on_node(i)


# Invoke after benchmarking is finished.
def remove_stopped_containers():
    # Remove any stopped containers.
    run('ansible -i {} master -a "sudo docker prune"'.format(to_harmonic_io_setup_path('hosts')), shell=True)


def ensure_exactly_containers(count):
    stop_all_containers_and_restart_hio()
    start_containers(count)
    call_ansible('sudo docker ps', hosts='workers')

def print_result(str):
    # append a line of text:
    print(str)
    with open(result_filename, "a") as file:
        file.write(str)
        file.write('\n')

def run_simulator():
    # TODO: simulator hostname as const
    # Streams 500 images, then polls MongoDB to wait for completion, and outputs benchmark info as CSV to stdout:
    completed_process = call_ansible('python3 ./exjobb/benchmark_full_pipeline.py', hosts=SIMULATOR_HOSTNAME)

    stdout = completed_process.stdout.decode('utf-8')

    benchmarks = [row.split(',') for row in stdout.splitlines() if row.startswith('benchmarking,')]
    #print(benchmarks)

    # 3rd column is the 'tag'.
    benchmark_total = [row for row in benchmarks if row[2] == 'full'][0]
    print(benchmark_total)

    return dict(zip(benchmarks[0], benchmark_total))

    # TODO:
    # - benchmark full pipeline needs to use the correct image
    # - need to record the output back somehow, grab the text - then process back here.
    # - output the results
    # run and record
    # stop and review
    # tidy up data

def run_test(container_count):
    print('container count is: {}'.format(container_count))
    ensure_exactly_containers(container_count)

    # Allow the master to update its status, and the system to stabilize:
    time.sleep(10)

    result = run_simulator()

    result['container_count'] = container_count

    print_result(str(result))

    # 6th column is the total time:

def benchmarks():
    # (Assuming that deployHIO has been run.)

    # Pull latest image we will use for benchmarking:
    call_ansible('sudo docker pull {}'.format(DOCKER_IMAGE_URL), hosts='workers')

    for container_count in range(1, NUMBER_WORKER_NODES + 1):
        run_test(container_count)


def ensure_normal_production_state():

    stop_all_containers_and_restart_hio()

    # TODO: what if the image name was different for the normal production state?
    # CONSTS for production state.
    ensure_exactly_containers(6)


if __name__ == '__main__':
    benchmarks()
    ensure_normal_production_state()
