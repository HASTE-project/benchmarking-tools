import http.server
import json
import threading
from .shared_state import shared_state, shared_state_lock, regenerate_data

PORT = 8080


# Binds to all network interfaces by default
# from https://gist.github.com/mdonkers/63e115cc0c79b4f6b8b3a6b797e485c7

# POST to '/' to set parameters:
# {
# 	"cpu_pause_ms": 100,
# 	"message_bytes": 20,
# 	"period_sec": 0.2
# }




# Must pass handler as a CLASS, capture state in closure:
class Handler(http.server.BaseHTTPRequestHandler):

    def _set_response(self):
        self.send_response(200)
        # TODO: should use JSON mime type
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        #print("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self._set_response()
        #self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))

    def do_POST(self):

        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        body = post_data.decode('utf-8')
        # TODO: check JSON mime type

        # print("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
        #       str(self.path), str(self.headers), body)

        body_parsed = json.loads(body)

        with shared_state_lock:
            # Just overwrite, the other thread only reads.
            # TODO: validate types.
            for key in ['cpu_pause_ms', 'message_bytes', 'period_sec']:
                if key in body_parsed:
                    shared_state['params'][key] = body_parsed[key]
                else:
                    print('missing key in POST data: ' + key)
            regenerate_data()

        print('new configuration is: ' + str(shared_state['params']))

        self._set_response()
        self.wfile.write("{'result':'new config applied.'}".encode('utf-8'))


def run():
    http_server_thread = threading.Thread(target=__run)
    http_server_thread.start()


def __run():
    # with not supported in Python3.5 for the httpd object:
    # TODO: migrate to 'with' when ubuntu has 2.6

    httpd = http.server.HTTPServer(('', PORT), Handler)
    print("HTTP control server listening on :", PORT)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print('Stopping httpd...')


if __name__ == '__main__':
    run()
