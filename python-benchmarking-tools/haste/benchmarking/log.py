
import time


def log(topic, message):
    log_line = time.time().strftime('{%Y-%m-%d %H:%M:%S}')
    print(log_line)