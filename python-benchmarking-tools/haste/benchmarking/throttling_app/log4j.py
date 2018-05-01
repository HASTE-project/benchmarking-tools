import re
import calendar, time

_log4j_re = re.compile('^([0-9]{4}-.{14})\s([A-Z]+)\s+([^:]+):([0-9]+)\s-\s(.*)[\n\r]*$')

def parse_log4j_line(line):
    result = {
        'line': line
    }

    match = _log4j_re.search(line)

    if match is not None:
        result['timestamp_str'] = match.group(1)
        result['level'] = match.group(2)
        result['logger_name'] = match.group(3)
        result['linenumber'] = match.group(4)
        result['message'] = match.group(5)

        result['timestamp_unix'] = calendar.timegm(time.strptime(result['timestamp_str'], '%Y-%m-%d %H:%M:%S'))

    return result


if __name__ == '__main__':
    examples = ["2018-04-29 08:41:25 INFO  file-streaming-app:76 - test log message\n", '', ' bla bla']
    for line in examples:
        r = parse_log4j_line(line)
        print(r)
