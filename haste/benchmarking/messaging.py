import random
import string
from itertools import repeat

RANDOM_1MB = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
                     for _ in range(1024))
RANDOM_100MB = ''.join(list(repeat(RANDOM_1MB, 100 * 1024)))


def generate_message(shared_state_copy):
    return bytes("C%06d-%s\n" % (shared_state_copy['cpu_pause_ms'],
                               RANDOM_100MB[:(shared_state_copy['message_bytes'] - 8)]),
                 'UTF-8')


def parse_message(line):
    return {'cpu_pause_ms': int(line[1:7])}


if __name__ == '__main__':
    shared_state = {'cpu_pause_ms': 123, 'message_bytes': 30000000}
    line = generate_message(shared_state)

    if len(line) != 30000000 + 1:  # account for \n
        print(len(line))
        raise Exception('generated message is wrong length')
    print('generated message is correct length')

    parsed = parse_message(line)
    if parsed['cpu_pause_ms'] != 123:
        raise Exception('CPU pause failed round trip conversion')
    print('CPU pause completed round trip conversion')
