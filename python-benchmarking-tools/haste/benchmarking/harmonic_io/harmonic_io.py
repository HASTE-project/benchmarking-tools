from subprocess import run, PIPE
import socket
import time

"""
Various helper functions for remote SSH invokation, setting up and tearing down HIO and associated containers.
"""

# Path to https://github.com/HASTE-project/HarmonicIOSetup
HARMONIC_IO_SETUP_PATH = '/Users/benblamey/projects/HASTE/HarmonicIOSetup/{}'
NUMBER_WORKER_NODES = 10
DOCKER_IMAGE_URL = 'benblamey/haste-image-proc:latest'

# TODO: factor out port numbers


# These hosts need to be defined in 'hosts' in HarmonicIOSetup, and SSH access configured.
def worker_hostname(i):
    # i start with 1
    return 'hio-worker-prod-0-{}'.format(i)


def worker_ip_address(i):
    # works because I have setup my /etc/hosts file with the private IPs.
    return socket.gethostbyname(worker_hostname(i))


def to_harmonic_io_setup_path(filename):
    return HARMONIC_IO_SETUP_PATH.format(filename)


def run_remote_ssh(cmd_str, hosts='workers:master', become=False):
    """
    :param cmd_str: command to run, double-quotes must be double-escaped: \\\"
    :param hosts:
    :return:
    """
    # shell = True means we don't have to specify array of strings for arguments
    proc_result = run('ansible -i {} {} {} -a "{}"'.format(
        to_harmonic_io_setup_path('hosts'),
        '--become' if become else '',
        hosts,
        cmd_str),
        shell=True, stdout=PIPE, stderr=PIPE)
    print(proc_result.stdout.decode('utf-8'))
    print(proc_result.stderr.decode('utf-8'))
    return proc_result


def run_playbook(playbook_filename):
    # shell = True means we don't have to specify array of strings for arguments
    proc_result = run(
        'ansible-playbook -i {} {}'.format(
            to_harmonic_io_setup_path('hosts'),
            to_harmonic_io_setup_path('playbooks/' + playbook_filename)),
        shell=True, stdout=PIPE, stderr=PIPE)
    print(proc_result.stdout.decode('utf-8'))
    print(proc_result.stderr.decode('utf-8'))
    return proc_result


def stop_all_containers_and_restart_hio():
    # this will clear HIO queues, metadata, etc.

    # stop the HIO master and worker (by quiting screen):
    run_playbook('stopMasterWorker.yml')

    run_remote_ssh('pkill -f harmonicIO', become=True)

    # stop any running containers: (--quiet = show only numeric IDs)
    # send SIGTERM, after grace period, SIGKILL
    run_remote_ssh('docker stop $(sudo docker ps --all --quiet --filter=\'name={}\')'.format(DOCKER_IMAGE_URL),
                   become=True)

    # Start HIO master and workers again
    run_playbook('startMasterWorker.yml')

    # check they are running (by checking for listening ports)
    run_remote_ssh(
        'sh -c \'netstat --numeric --listening --tcp | grep --line-buffered --extended \\\"(8888|8080)\\\"\'')

    # TODO: capture output string from netstat to double-check number of workers/master. !!!!!!!!!!!

    # Allow system to stabilize
    time.sleep(10)


def pull_docker_image():
    run_remote_ssh('docker pull %s' % DOCKER_IMAGE_URL, hosts='workers', become=True)


def start_containers_on_node(i, count=1):
    # note curly braces for JSON are doubled to escape them.
    # note that HIO worker does not bind to localhost, we need to use the actual IP address.
    # See: https://github.com/HASTE-project/HarmonicIO/issues/4

    # We can invoke this from outside, but to save the firewall faff, do it from the remote shell.

    # TODO: just run this on, say, the simulator node?
    run_remote_ssh(
        'curl -X POST \\\"http://{}:8888/docker?token=None&command=create\\\" --data \'{{\\\"c_name\\\" : \\\"{}\\\", \\\"num\\\" : {}}}\''
            .format(worker_ip_address(i), DOCKER_IMAGE_URL, count), hosts='lovisainstance')


def start_containers(count):
    for i in range(1, count + 1):
        start_containers_on_node(i)


# Invoke after benchmarking is finished.
def remove_stopped_containers():
    # Remove any stopped containers.
    # --force - don't prompt to confirm
    run_remote_ssh('sudo docker container prune --force', hosts='workers')


def ensure_exactly_containers(count):
    # TODO remove this line?
    #stop_all_containers_and_restart_hio()
    start_containers(count)

    # Allow the master to update its status, and the system to stabilize:
    time.sleep(10)

    run_remote_ssh('sudo docker ps', hosts='workers')


def ensure_normal_production_state():
    stop_all_containers_and_restart_hio()

    pull_image_and_start_containers()


def pull_image_and_start_containers():
    pull_docker_image()

    # TODO: what if the image name was different for the normal production state?
    for i in range(1, NUMBER_WORKER_NODES + 1):
        start_containers_on_node(i, count=1)


if __name__ == '__main__':
    pull_image_and_start_containers()
