from .shared_state import shared_state, shared_state_lock
import os
import time
import shutil
import platform
import threading

_FILENAME_PREFIX = ".COPYING." #filenames starting with a . are ignored by Spark
_REPORT_INTERVAL = 5
_counter = 0
_USE_RAMDISK = False

if platform.system() == 'Darwin':
    # on my laptop:
    WORKING_DIR_BASE = '/tmp/benchmarking/'
else:
    if _USE_RAMDISK:
        WORKING_DIR_BASE = '/srv/nfs-export/shm/benchmarking/'
    else:
        WORKING_DIR_BASE = '/srv/nfs-export/bench2/'

os.makedirs(WORKING_DIR_BASE, exist_ok=True)


def start():
    thread_delete_old_files = threading.Thread(target=_start_deleting_old_files)
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
            if not entry.name.startswith('.') and not entry.name.endswith('_COPYING') and entry.is_file():
                #print(entry.name)
                stat = entry.stat()
                access_time_seconds = stat.st_atime
                #print(access_time_seconds)
                if now > 10 + access_time_seconds:
                    #print('deleting..' + entry.name)
                    os.remove(WORKING_DIR_BASE + entry.name)
        #it.close() needs >py3.6
        time.sleep(1)


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
        create_new_file(message_bytes, WORKING_DIR_BASE)

        message_count = message_count + 1

        ts_after_stream = time.time()

        pause = shared_state_copy['params']['period_sec'] - (ts_after_stream - ts_before_stream)

        if pause > 0:
            time.sleep(pause)
        # else:
        #   print('streaming_server: overran target period by ' + str(-pause) + ' seconds!')

        if int(ts_before_stream) >= last_unix_time_interval + _REPORT_INTERVAL:
            last_unix_time_interval = int(ts_before_stream)
            print('streamed ' + str(message_count) + ' messages to ' + WORKING_DIR_BASE
                  + ' , reporting every ' + str(_REPORT_INTERVAL) + ' seconds')


def create_new_file(message_bytes, working_dir):
    filename = generate_filename()
    file_path_without_suffix = working_dir + '/' + filename
    file_path_with_suffix = working_dir + '/' + _FILENAME_PREFIX + filename


    file = open(file_path_with_suffix, "wb")
    file.write(message_bytes)
    file.close()

    # Clean rename so we pick it up safely in Spark
    os.rename(file_path_with_suffix, file_path_without_suffix)

    return filename, file_path_with_suffix


def generate_filename():
    global _counter
    _counter = _counter + 1
    filename = "%s_%08d" % (str(time.time()), _counter % 10000000)
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
