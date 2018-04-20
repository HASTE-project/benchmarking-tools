from .shared_state import shared_state, shared_state_lock
import os
import time
import shutil
import platform
import threading
from ..messaging import generate_message
import json

SEARCH_FOR_OLD_FILES = 1
DELETE_OLD_FILES_AFTER = 150  # 2 x batch interval + forced garbage collection interval + margin

_FILENAME_PREFIX = ".COPYING." #filenames starting with a . are ignored by Spark
_REPORT_INTERVAL = 3
_counter = 0
_USE_RAMDISK = False

if platform.system() == 'Darwin':
    # on my laptop:
    WORKING_DIR_BASE = '/tmp/benchmarking/'
else:
    if _USE_RAMDISK:
        # (This is a bind mount from /dev/shm/bench - to where its mounted on the clients
        WORKING_DIR_BASE = '/mnt/nfs/ben-stream-src-2-shm-bench/'
    else:

        WORKING_DIR_BASE = '/srv/nfs-export/bench2/'

os.makedirs(WORKING_DIR_BASE, exist_ok=True)

# TODO: delete all files in directory


MESSAGE_SIZES = [500, 1000, 10000, 100000, 1000000, 5000000, 10000000]

# TODO: symlink approach
# for message_size in MESSAGE_SIZES:
#     message_bytes = generate_message()
#     create_new_file()

_file_paths = {}

def get_path_of_original(shared_state):
    key = json.dumps(shared_state['params'], sort_keys=True)

    if key in _file_paths:

        path = _file_paths[key]

        # # 'touch' the file so its picked up by spark
        # with open(path, 'a'):
        #     try:  # Whatever if file was already existing
        #         os.utime(path, None)  # => Set current time anyway
        #     except OSError:
        #         pass  # File deleted between open() and os.utime() calls
        # update - not needed - instead we create new files.

        return path
    else:
        file_path_with_prefix = WORKING_DIR_BASE + '.ORIG.' + generate_filename()
        file = open(file_path_with_prefix, "wb")
        file.write(shared_state['message'])
        file.close()
        _file_paths[key] = file_path_with_prefix
        return file_path_with_prefix


def start():
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
            if 'COPYING' not in entry.name and entry.is_file():
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
                    #print('deleting..' + entry.name)
                    os.remove(WORKING_DIR_BASE + entry.name)
        #it.close() needs >py3.6
        time.sleep(SEARCH_FOR_OLD_FILES)
        #print('looking for files to delete...')


def _start_file_streaming():
    last_unix_time_interval = -1
    last_unix_time_second = -1
    message_count = 0

    while True:
        ts_before_stream = time.time()

        if last_unix_time_second != int(ts_before_stream):
            last_unix_time_second = int(ts_before_stream)

            with shared_state_lock:
                shared_state_copy = shared_state.copy()
                # TODO: don't send the exact same string each time (incase Spark caches it)
                message_bytes = shared_state['message']

        # TODO: this includes the '\n' at the end (the length is one byte off)
        # TODO: save message to disk
        #create_new_file(message_bytes, WORKING_DIR_BASE)

        filepath_of_original = get_path_of_original(shared_state_copy)

        # looks like apache spark doesn't support soft links
        create_hardlink(filepath_of_original)

        message_count = message_count + 1

        ts_after_stream = time.time()

        pause = shared_state_copy['params']['period_sec'] - (ts_after_stream - ts_before_stream)

        if pause > 0:
            time.sleep(pause)
        else:
            print('streaming_server: overran target period by ' + str(-pause) + ' seconds!')

        if int(ts_before_stream) >= last_unix_time_interval + _REPORT_INTERVAL:

            # also, use the reporting interval to make new 'core' files - otherwise we re-process the old files.
            # this works if the reporting interval is less than the batch interval
            global _file_paths
            _file_paths = {}

            last_unix_time_interval = int(ts_before_stream)
            print('streamed ' + str(message_count) + ' messages to ' + WORKING_DIR_BASE
                  + ' , reporting every ' + str(_REPORT_INTERVAL) + ' seconds')


def create_hardlink(filepath_of_original):
    filename = generate_filename()
    file_path_without_prefix = WORKING_DIR_BASE + '/' + filename
    file_path_with_prefix = WORKING_DIR_BASE + '/' + _FILENAME_PREFIX + filename

    os.link(filepath_of_original, file_path_with_prefix)

    # Clean rename so we pick it up safely in Spark
    os.rename(file_path_with_prefix, file_path_without_prefix)


def create_new_file(message_bytes, working_dir):
    filename = generate_filename()
    file_path_without_prefix = working_dir + '/' + filename
    file_path_with_prefix = working_dir + '/' + _FILENAME_PREFIX + filename


    file = open(file_path_with_prefix, "wb")
    file.write(message_bytes)
    file.close()

    # Clean rename so we pick it up safely in Spark
    os.rename(file_path_with_prefix, file_path_without_prefix)

    return filename, file_path_with_prefix


def generate_filename():
    global _counter
    _counter = _counter + 1
    filename = "{0:}_{1:0>10}".format(int(time.time()), _counter % 1000000)
    #filename = "%s_{}" % (int(time.time()), _counter % 1000000)
    return filename


if __name__ == '__main__':
    start()

    if False:
        # for 3 000 000 byte files (3 MB)
        # ubuntu@ben-stream-src:~/benchmarking-tools/haste$ python3 -m haste.benchmarking.file_streaming
        # 0.006684303283691406 - one file
        # 0.7554028034210205 - copying 200 files
        # ubuntu@ben-stream-src:~/benchmarking-tools/haste$ python3 -m haste.benchmarking.file_streaming
        # 0.011159896850585938
        # 2.115196943283081 - copying 200 files


        MESSAGE_SIZES = [5000000]

        for message_size in MESSAGE_SIZES:
            print(message_size)

            config = {'cpu_pause_ms': 20, 'message_bytes': message_size}
            working_dir = WORKING_DIR_BASE + str(message_size) + '/'

            os.makedirs(working_dir, exist_ok=True)

            filename_base, filename_with_suffix = create_new_file(config, working_dir, filename_suffix='')

            #number_of_files = min(max(int((15 * 1000000) / message_size), 10),10000)
            number_of_files = 200

            ts_start_file_streaming = time.time()

            for i in range(number_of_files):
                shutil.copy(filename_with_suffix, filename_with_suffix + '_' + str(i))

            print(time.time() - ts_start_file_streaming)

    #
    #

    # print(time.time() - start)
    #
    # #
    # # start = time.time()
    # # os.system('cp source.txt destination.txt')
    # # print(time.time() - start)
