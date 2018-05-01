import urllib.request
import json


def post_new_status(host, port, json_str_to_post):
    req = urllib.request.Request("http://%s:%d/api/v1/applications" % (host, port),
                                 json_str_to_post,
                                 headers={'Content-type': 'application/json',
                                          'Accept': 'application/json'})

    f = urllib.request.urlopen(req)
    response = f.read()
    return response


if __name__ == '__main__':
    shared_state = {
        'params': {
            'cpu_pause_ms': 20,
            'message_bytes': 200,
            'period_sec': 1.0,
        }
    }
    resp = post_new_status('localhost', 8080, json.dumps(shared_state).encode('utf-8'))
    print(resp)
