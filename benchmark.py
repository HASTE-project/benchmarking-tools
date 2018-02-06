from subprocess import call

#
# 1) deployHIO
#
#
#


NUMBER_WORKER_NODES = 6
DOCKER_IMAGE_URL = 'benblamey/haste-image-proc'

# ansible -i hosts workers:master -a "sh -c 'netstat --numeric --listening --tcp | grep --line-buffered --extended \"(8080|8888)\"'"

# (Assuming that deployHIO has been run.)

# Pull latest image we will use for benchmarking:
# shell = True means we don't have to specify array of strings for arguments
call('ansible -i ../HarmonicIOSetup/hosts workers:master -a "sudo docker pull {}"'.format(DOCKER_IMAGE_URL), shell=True)

#for num_containers in range(1, NUMBER_WORKER_NODES + 1):

# stop the HIO master and worker (by quiting screen):
call('ansible-playbook -i ../HarmonicIOSetup/hosts ../HarmonicIOSetup/playbooks/stopMasterWorker.yml', shell=True)

# stop any running containers:
call('ansible -i ../HarmonicIOSetup/hosts workers:master -a "sudo docker stop $(sudo docker ps -a -q --filter=\'name={}\')"'
     .format(DOCKER_IMAGE_URL), shell=True)

call('ansible -i ../HarmonicIOSetup/hosts workers:master -a "sudo docker kill $(sudo docker ps -a -q --filter=\'name={}\')"'
     .format(DOCKER_IMAGE_URL), shell=True)

# Start HIO master and workers again
call('ansible-playbook -i ../HarmonicIOSetup/hosts ../HarmonicIOSetup/playbooks/startMasterWorker.yml', shell=True)

# check they are running
call('ansible -i ../HarmonicIOSetup/hosts workers:master -a "sh -c \'netstat --numeric --listening --tcp | grep --line-buffered --extended \\\"(8888|8080)\\\"\'"', shell=True)
# TODO: capture output string and check it is working.



# After the run

# Remove any stopped containers.
call('ansible -i ../HarmonicIOSetup/hosts workers:master -a "sudo docker prune"', shell=True)


# back to clean state
# revert to production environment



# for each run
#          - pull the container I want
#
# for each parameter set
# - stop hio workers
# - stop hio masters
#
# - stop containers
# docker stop $(docker ps -aq)
# docker kill $(docker ps -aq)
# - docker container prune
#
#
# - start hio master
# - start hio worker
#
# (pause)
#
# - start 1 container (for prod pipeline)
# - start N containers for the profiling
#
# - run the profiling
# - invoke the simulator
# - wait for completion
