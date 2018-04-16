from .streaming_server_socket import start_socket_streaming
from .control_http_server import run_control_server
from .file_streaming import start
import threading

# Dummy Server for Benchmarking Streaming Applications.
# streaming_server_socket - streams lines of text on TCP port 9999
# control_http_server - listens for HTTP POSTs on 8080 to vary parameters (format below)


TCP_OR_FILE = False

if __name__ == '__main__':
    run_control_server()

    if TCP_OR_FILE:
        start_socket_streaming()
    else:
        start()
