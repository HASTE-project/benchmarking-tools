import time

def pause(secs):
    start = time.time()
    while time.time() < start + secs:
        #print('.')
        x = 0
        for n in range(2000):
            x = x + 1


if __name__ == '__main__':
    pause(0.001)