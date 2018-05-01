import random
import string
from itertools import repeat
import time

RANDOM_1KB = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
                     for _ in range(1000))
RANDOM_100MB = bytearray(''.join(list(repeat(RANDOM_1KB, 100 * 1024))), 'utf-8')

NEWLINE = bytes("\n", 'UTF-8')[0] # 10

_counter = 0


# This takes ~0.04 seconds! but we only need to do it each time we change the params
def generate_message(shared_state_copy, newline_terminator=True):
    global _counter
    _counter += 1
    filename = "%s_%08d" % (str(time.time()), _counter & 10000000)
    content = "C%06d-F%s-" % (shared_state_copy['cpu_pause_ms'], filename)
    content_bytes = bytearray(content, 'UTF-8')

    length = shared_state_copy['message_bytes'] - (len(content_bytes))

    if newline_terminator:
        length = length + 1

    content_bytes.extend(RANDOM_100MB[:length])

    if newline_terminator:
        content_bytes[-1] = NEWLINE

    return content_bytes, filename


def parse_message(line):
    return {'cpu_pause_ms': int(line[1:7])}


if __name__ == '__main__':
    shared_state = {'cpu_pause_ms': 123, 'message_bytes': 30000000}

    time_start = time.time()
    line, filename = generate_message(shared_state)
    print(time.time() - time_start)

    if len(line) != 30000000 + 1:  # account for \n
        print(len(line))
        raise Exception('generated message is wrong length')
    print('generated message is correct length')

    parsed = parse_message(line)
    if parsed['cpu_pause_ms'] != 123:
        raise Exception('CPU pause failed round trip conversion')
    print('CPU pause completed round trip conversion')
