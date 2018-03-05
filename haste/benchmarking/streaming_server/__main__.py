from .streaming_server_socket import start_streaming_server
from .control_http_server import run
import threading

# Dummy Server for Benchmarking Streaming Applications.
# streaming_server_socket - streams lines of text on TCP port 9999
# control_http_server - listens for HTTP POSTs on 8080 to vary parameters (format below)




if __name__ == '__main__':
    run()
    start_streaming_server()
