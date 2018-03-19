import time

def pause(secs):
    start = time.time()
    while time.time() < start + secs:
        #print('.')
        x = 0
        # TODO: this might get optimized away, do something more fancy - with rand() etc.
        for n in range(200):
            x = x + 1


if __name__ == '__main__':
    pause(0.001)