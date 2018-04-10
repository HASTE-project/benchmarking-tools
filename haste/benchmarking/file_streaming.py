from . import messaging
import os
import time
import shutil

# python3 -m haste.benchmarking.file_streaming

#WORKING_DIR = '/mnt/ramdisk/benchmarking/'
WORKING_DIR = '/tmp/benchmarking/'

# for 3 000 000 byte files (3 MB)
# ubuntu@ben-stream-src:~/benchmarking-tools/haste$ python3 -m haste.benchmarking.file_streaming
# 0.006684303283691406 - one file
# 0.7554028034210205 - copying 200 files
# ubuntu@ben-stream-src:~/benchmarking-tools/haste$ python3 -m haste.benchmarking.file_streaming
# 0.011159896850585938
# 2.115196943283081 - copying 200 files

os.makedirs(WORKING_DIR, exist_ok=True)


def create_new_file(filename_suffix=''):
    bytes, filename = messaging.generate_message({'cpu_pause_ms': 20, 'message_bytes': 3000000},
                                                 newline_terminator=False)

    filename_with_suffix = WORKING_DIR + filename + filename_suffix
    newFile = open(filename_with_suffix, "wb")
    newFile.write(bytes)
    newFile.close()

    return (filename, filename_with_suffix)


start = time.time()
filename_base, filename_with_suffix = create_new_file(filename_suffix='IGNORE')

print(time.time() - start)

start = time.time()
for i in range(200):
    shutil.copy(filename_with_suffix, WORKING_DIR + '_' + str(i))
print(time.time() - start)

#
# start = time.time()
# os.system('cp source.txt destination.txt')
# print(time.time() - start)
