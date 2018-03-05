import socket
import threading
import time
from .shared_state import shared_state, shared_state_lock
from ..messaging import generate_message

REPORT_INTERVAL = 5

ALL_HOSTS = ""
STREAM_PORT = 9999
CONTROL_PORT = 8889

class ClientStreamingThread(threading.Thread):

    def __init__(self, client_address, client_socket):
        threading.Thread.__init__(self)
        self.csocket = client_socket
        self.client_address = client_address
        self.client_socket = client_socket

    def run(self):
        print("Stream Connection from : ", self.client_address)

        # self.csocket.send(bytes("Hi, This is from Server..", 'utf-8'))

        last_unix_time_interval = -1
        last_unix_time_second = -1
        message_count = 0

        # data = self.csocket.recv(2048)
        # msg = data.decode()
        # if msg == 'bye':
        #     break
        # print("from client", msg)

        while True:

            ts_before_stream = time.time()

            if last_unix_time_second != int(ts_before_stream):
                last_unix_time_second = int(ts_before_stream)

                with shared_state_lock:
                    shared_state_copy = shared_state.copy()
                    # TODO: don't send the exact same string each time (incase Spark caches it)
                    message_to_send = shared_state['message']

            self.csocket.send(message_to_send)
            message_count = message_count + 1

            ts_after_stream = time.time()

            pause = shared_state_copy['params']['period_sec'] - (ts_after_stream - ts_before_stream)

            if pause > 0:
                time.sleep(pause)
            else:
                print('streaming_server: overran target period by ' + str(-pause) + ' seconds!')

            if int(ts_before_stream) >= last_unix_time_interval + REPORT_INTERVAL:
                last_unix_time_interval = int(ts_before_stream)
                print('streamed ' + str(message_count) + ' messages to ' + str(self.client_address)
                      + ' , reporting every ' + str(REPORT_INTERVAL) + ' seconds')




# Shared state looks like this:
# shared_state = {
#     'params': {
#         'cpu_pause_ms': 20,
#         'message_bytes': 200,
#         'period_sec': 0.0002,
#     }
# }


def start_streaming_server():
    stream_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stream_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    stream_server.bind((ALL_HOSTS, STREAM_PORT))
    print("Streaming Server started on " + ALL_HOSTS + ":" + str(STREAM_PORT))
    print("Waiting for client request..")
    while True:
        stream_server.listen()
        client_socket, client_address = stream_server.accept()
        new_thread = ClientStreamingThread(client_address, client_socket)
        new_thread.start()


if __name__ == '__main__':
    start_streaming_server()
