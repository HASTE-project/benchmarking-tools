from . import messaging
import os
import time
import shutil

# python3 -m haste.benchmarking.file_streaming

# WORKING_DIR_BASE = '/srv/nfs-export/benchmarking/'
# WORKING_DIR_BASE = '/tmp/benchmarking/'
WORKING_DIR_BASE = '/srv/nfs-export/shm/benchmarking'

# for 3 000 000 byte files (3 MB)
# ubuntu@ben-stream-src:~/benchmarking-tools/haste$ python3 -m haste.benchmarking.file_streaming
# 0.006684303283691406 - one file
# 0.7554028034210205 - copying 200 files
# ubuntu@ben-stream-src:~/benchmarking-tools/haste$ python3 -m haste.benchmarking.file_streaming
# 0.011159896850585938
# 2.115196943283081 - copying 200 files

os.makedirs(WORKING_DIR_BASE, exist_ok=True)


def create_new_file(config, working_dir, filename_suffix=''):
    bytes, filename = messaging.generate_message(config,
                                                 newline_terminator=False)

    filename_with_suffix = working_dir + filename + filename_suffix
    newFile = open(filename_with_suffix, "wb")
    newFile.write(bytes)
    newFile.close()

    return (filename, filename_with_suffix)


start = time.time()

#MESSAGE_SIZES = [500, 1000, 10000, 100000, 1000000, 5000000, 10000000]
MESSAGE_SIZES = [5000000]

for message_size in MESSAGE_SIZES:
    print(message_size)

    config = {'cpu_pause_ms': 20, 'message_bytes': message_size}
    working_dir = WORKING_DIR_BASE + str(message_size) + '/'

    os.makedirs(working_dir, exist_ok=True)

    filename_base, filename_with_suffix = create_new_file(config, working_dir, filename_suffix='')

    #number_of_files = min(max(int((15 * 1000000) / message_size), 10),10000)
    number_of_files = 200

    start = time.time()

    for i in range(number_of_files):
        shutil.copy(filename_with_suffix, filename_with_suffix + '_' + str(i))

    print(time.time() - start)

#
#

# print(time.time() - start)
#
# #
# # start = time.time()
# # os.system('cp source.txt destination.txt')
# # print(time.time() - start)
