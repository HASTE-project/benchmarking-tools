from subprocess import call
import socket

# Path to https://github.com/HASTE-project/HarmonicIOSetup
HARMONIC_IO_SETUP_PATH = '../HarmonicIOSetup/{}'
NUMBER_WORKER_NODES = 6
DOCKER_IMAGE_URL = 'benblamey/haste-image-proc'
# TODO: factor out port numbers


def worker_hostname(i):
    # start with 1
    return 'hio-worker-prod-{}'.format(i)


def worker_ip_address(i):
    # works because I have setup my /etc/hosts file with the private IPs.
    return socket.gethostbyname(worker_hostname(i))


def worker_ansible_name(i):
    # start with 1
    return 'workernode{}'.format(i)


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
    return call('ansible -i ../HarmonicIOSetup/hosts {} -a "{}"'.format(hosts, cmd_str), shell=True)

# run_playbook
def call_ansible_playbook(playbook_filename):
    # shell = True means we don't have to specify array of strings for arguments
    return call(
        'ansible-playbook -i ../HarmonicIOSetup/hosts ../HarmonicIOSetup/playbooks/{}'.format(playbook_filename),
        shell=True)


# (Assuming that deployHIO has been run.)


def stop_all_containers_and_restart_hio():

    # Pull latest image we will use for benchmarking:
    #call_ansible('sudo docker pull {}'.format(DOCKER_IMAGE_URL))

    # for num_containers in range(1, NUMBER_WORKER_NODES + 1):

    # stop the HIO master and worker (by quiting screen):
    call_ansible_playbook('stopMasterWorker.yml')

    # stop any running containers:
    call_ansible('sudo docker stop $(sudo docker ps -a -q --filter=\'name={}\')'.format(DOCKER_IMAGE_URL))

    call_ansible('sudo docker kill $(sudo docker ps -a -q --filter=\'name={}\')'.format(DOCKER_IMAGE_URL))

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
    call('ansible -i {} master -a "sudo docker prune"'.format(to_harmonic_io_setup_path('hosts')), shell=True)


def ensure_exactly_containers(count):
    stop_all_containers_and_restart_hio()
    start_containers(count)
    call_ansible('sudo docker ps')


def run_simulator():
    call_ansible('python3 ./exjobb/benchmark_full_pipeline.py', hosts='lovisainstance')
    # TODO:
    # - benchmark full pipeline needs to use the correct image
    # - need to record the output back somehow, grab the text - then process back here.
    # - output the results
    # run and record
    # stop and review
    # tidy up data


def benchmark():
    for container_count in range(1, NUMBER_WORKER_NODES + 1):
        print('container count is: {}'.format(container_count))
        ensure_exactly_containers(container_count)
        run_simulator()
        # run the simulator

def ensure_normal_production_state():
    # TODO:
    # back to clean state
    # revert to production environment
    # CONSTS for production state.
    pass


if __name__ == '__main__':
    #print(worker_ip_address(1))
    #ensure_exactly_containers(2)
    # start_containers(3)
    # call_ansible('sudo docker ps')

    benchmark()

    ensure_normal_production_state()
