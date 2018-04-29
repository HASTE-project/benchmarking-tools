from .shared_state import shared_state, shared_state_lock

from ...spark.file_streaming_benchmark import USE_RAMDISK, BATCH_INTERVAL_SECONDS
import os
import time
import shutil
import platform
import threading
import json

SEARCH_FOR_OLD_FILES_INTERVAL = 1
MESSAGE_SIZES = [500, 1000, 10000,
                 100000,
                 1000000,  # 1MB
                 #5000000,  # 5MB
                 #10000000,  # 10MB
                 #50000000,  # 50MB
                 #100000000  # 100MB
                 ]

_FILENAME_IGNORE_PREFIX = ".COPYING." #filenames starting with a . are ignored by Spark
_REPORT_INTERVAL = 3
_counter = 0
_USE_HARD_LINKS = False

_file_paths = {}

# 6 x batch interval (+margin if small)
DELETE_OLD_FILES_AFTER = BATCH_INTERVAL_SECONDS * 12

if platform.system() == 'Darwin':
    # on my laptop:
    WORKING_DIR_BASE = '/tmp/benchmarking/'
else:
    if USE_RAMDISK:
        # (This is a bind mount from /dev/shm/bench - to where its mounted on the clients
        WORKING_DIR_BASE = '/dev/shm/bench/'
    else:
        WORKING_DIR_BASE = '/srv/nfs-export/bench2/'


# TODO: symlink approach
# for message_size in MESSAGE_SIZES:
#     message_bytes = generate_message()
#     create_new_file()


def start():
    os.makedirs(WORKING_DIR_BASE, exist_ok=True)

    # delete all files in directory
    print('deleting all files in ' + WORKING_DIR_BASE)
    for f in os.listdir(WORKING_DIR_BASE):
        file_path = os.path.join(WORKING_DIR_BASE, f)
        if os.path.isfile(file_path):
            os.unlink(file_path)

    thread_delete_old_files = threading.Thread(target=_start_deleting_old_files, daemon=True)
    thread_delete_old_files.start()

    thread_streaming = threading.Thread(target=_start_file_streaming)
    thread_streaming.start()
    thread_streaming.join()


def _start_deleting_old_files():
    # Careful with this one!!
    while True:
        now = time.time()
        it = os.scandir(WORKING_DIR_BASE)
        for entry in it:
            if _FILENAME_IGNORE_PREFIX not in entry.name and entry.is_file():
                #print(entry.name)

                # The file is a hard-link - delete it based on its creation time (not the file attributes)

                # parts = entry.name.split('_')
                # if len(parts) != 2:
                #     continue

                stat = entry.stat()
                # try:
                #     file_created = int(parts[0])
                # except ValueError:
                #     # skip file
                #     continue
                modification_time_seconds = stat.st_mtime
                # #print(access_time_seconds)


                # Think there is an issue if garbage collection occurs and the files have been removed:
                if now > DELETE_OLD_FILES_AFTER + modification_time_seconds:
                    #print(time.asctime() + ' deleting.. ' + entry.name)
                    os.remove(WORKING_DIR_BASE + entry.name)
        #it.close() needs >py3.6
        time.sleep(SEARCH_FOR_OLD_FILES_INTERVAL)
        #print('looking for files to delete...')


def _start_file_streaming():
    last_unix_time_interval = -1
    last_unix_time_second = -1
    message_count_all_time = 0
    message_count_this_interval = 0

    while True:
        ts_before_stream = time.time()

        # if last_unix_time_second != int(ts_before_stream):
        #     last_unix_time_second = int(ts_before_stream)

        # Get the message from shared state:
        with shared_state_lock:
            shared_state_copy = shared_state.copy()
            # TODO: don't send the exact same string each time (incase Spark caches it)
            frequency = 1/shared_state['params']['period_sec']

        print('frequency is ' + str(frequency))

        # TODO: this includes the '\n' at the end (the length is one byte off)
        # TODO: save message to disk

        for i in range(0, int(frequency)):
            create_file(shared_state_copy)
            message_count_all_time = message_count_all_time + 1
            message_count_this_interval = message_count_this_interval + 1

        ts_after_stream = time.time()

        pause = 1 - (ts_after_stream - ts_before_stream)

        if pause > 0:
            time.sleep(pause)
        else:
            print('streaming_server: overran target period by ' + str(-pause) + ' seconds!')
            pass

        if int(ts_before_stream) >= last_unix_time_interval + _REPORT_INTERVAL:
            # also, use the reporting interval to make new 'core' files - otherwise we re-process the old files.
            # this works if the reporting interval is less than the batch interval
            if _USE_HARD_LINKS:
                global _file_paths
                _file_paths = {}

            last_unix_time_interval = int(ts_before_stream)
            print('streamed ' + str(message_count_all_time) + ' messages to ' + WORKING_DIR_BASE
                  + ' , reporting every ' + str(_REPORT_INTERVAL) + ' seconds')
            #print(message_count_this_interval/_REPORT_INTERVAL, 1/shared_state_copy['params']['period_sec'])
            message_count_this_interval = 0






def create_file(shared_state):
    key = json.dumps(shared_state['params'], sort_keys=True)
    #print(key)

    new_filename = generate_filename()
    new_file_path_without_prefix = WORKING_DIR_BASE + new_filename

    if _USE_HARD_LINKS and key in _file_paths:
        # Create a new hardlink to an existing file:

        path_to_target = _file_paths[key]

        # (hard link is atomic)
        #a = time.time()
        os.link(path_to_target, new_file_path_without_prefix)
        #print(time.time() - a)

        return path_to_target
    else:
        #print('making a new file')
        # Make a new file
        new_file_path_with_prefix = WORKING_DIR_BASE + _FILENAME_IGNORE_PREFIX + new_filename

        # TODO: if this fails, kill the application

        #try:
        file = open(new_file_path_with_prefix, "wb")
        file.write(shared_state['message'])
        file.close()
        # except Exception:
        #     exit('failed to create new file')

        # Clean rename so we pick it up safely in Spark
        os.rename(new_file_path_with_prefix, new_file_path_without_prefix)

        _file_paths[key] = new_file_path_without_prefix
        return new_file_path_without_prefix


def generate_filename():
    global _counter
    _counter = _counter + 1
    filename = "{0:}_{1:0>10}".format(int(time.time()), _counter % 1000000)
    #filename = "%s_{}" % (int(time.time()), _counter % 1000000)
    return filename


if __name__ == '__main__':

    def create_new_file(message_bytes, working_dir):
        filename = generate_filename()
        file_path_without_prefix = working_dir + '/' + filename
        file_path_with_prefix = working_dir + '/' + _FILENAME_IGNORE_PREFIX + filename

        file = open(file_path_with_prefix, "wb")
        file.write(message_bytes)
        file.close()

        # Clean rename so we pick it up safely in Spark
        os.rename(file_path_with_prefix, file_path_without_prefix)

        return filename, file_path_with_prefix


    start()

    if False:
        # for 3 000 000 byte files (3 MB)
        # ubuntu@ben-stream-src:~/benchmarking-tools/haste$ python3 -m haste.benchmarking.file_streaming
        # 0.006684303283691406 - one file
        # 0.7554028034210205 - copying 200 files
        # ubuntu@ben-stream-src:~/benchmarking-tools/haste$ python3 -m haste.benchmarking.file_streaming
        # 0.011159896850585938
        # 2.115196943283081 - copying 200 files


        # MESSAGE_SIZES = [5000000]

        for message_size in MESSAGE_SIZES:
            #print(message_size)

            config = {'cpu_pause_ms': 20, 'message_bytes': message_size}
            working_dir = WORKING_DIR_BASE + str(message_size) + '/'

            os.makedirs(working_dir, exist_ok=True)

            filename_base, filename_with_suffix = create_new_file(config, working_dir, filename_suffix='')

            #number_of_files = min(max(int((15 * 1000000) / message_size), 10),10000)
            number_of_files = 200

            ts_start_file_streaming = time.time()

            for i in range(number_of_files):
                shutil.copy(filename_with_suffix, filename_with_suffix + '_' + str(i))

            #print(time.time() - ts_start_file_streaming)

    #
    #

    # print(time.time() - start)
    #
    # #
    # # start = time.time()
    # # os.system('cp source.txt destination.txt')
    # # print(time.time() - start)
