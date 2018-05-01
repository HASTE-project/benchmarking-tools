from sh import tail, pwd
import calendar, time
import re
from .log4j import parse_log4j_line

SPARK_APP_LOG = "../../../../spark-app.log"

# for line in Pygtail(SPARK_APP_LOG):
#     sys.stdout.write(line)

_example_log_line_count = "2018-04-29 08:41:25 INFO  file-streaming-app:76 - test log message"
_example_log_line_file_list = '2018-04-29 08:41:40 INFO  FileInputDStream:54 - Finding new files took 53 ms'
find_file_pauses_s_by_unixtime = {}


def _process_line(line):
    result = parse_log4j_line(line)

    if 'logger_name' not in result.keys():
        pass
    elif result['logger_name'] == 'file-streaming-app':
        print(line.strip())
        # TODO
        #processed_count_by_unixtime[result['timestamp_unix']] = new_files_took
    elif result['logger_name'] == 'FileInputDStream' and result['message'].find('Finding new files took') >= 0:
        new_files_took_ms = float(re.search('[0-9]+', result['message']).group(0))
        #print(line.strip())
        find_file_pauses_s_by_unixtime[result['timestamp_unix']] = new_files_took_ms / 1000
    else:
        pass


def _tail_log():
    for line in tail("-n", "500", SPARK_APP_LOG):
        _process_line(line)


def parse_spark_app_logs_and_update():
    _tail_log()


if __name__ == '__main__':
    #print(pwd())
    #tail_log()
    _process_line(_example_log_line_count)
    _process_line(_example_log_line_file_list)
    _process_line(' sdfsdf')

    parse_spark_app_logs_and_update()
    print(find_file_pauses_s_by_unixtime)
