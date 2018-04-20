import urllib.request
import json


def _get_and_parse_json(url):
    # print(url)
    req = urllib.request.Request(url,
                                 headers={'Content-type': 'application/json',
                                          'Accept': 'application/json'})
    f = urllib.request.urlopen(req)
    parsed = json.loads(f.read())
    # {'startTime': '2018-03-02T06:26:10.544GMT', 'batchDuration': 1000, 'numReceivers': 1, 'numActiveReceivers': 1,
    #  'numInactiveReceivers': 0, 'numTotalCompletedBatches': 29, 'numRetainedCompletedBatches': 29,
    #  'numActiveBatches': 0, 'numProcessedRecords': 28, 'numReceivedRecords': 28, 'avgInputRate': 0.9655172413793104,
    #  'avgSchedulingDelay': 5, 'avgProcessingTime': 122, 'avgTotalDelay': 127}
    return parsed


# {
# 	"cpu_pause_ms": 20,
# 	"message_bytes": 5000000,
# 	"period_sec": 0.03
# }

def set_new_params(params):
    params_as_json = json.dumps(params).encode('utf8')
    print(params_as_json)

    # conditionsSetURL = 'http://httpbin.org/post'
    # newConditions = {"con1": 40, "con2": 20, "con3": 99, "con4": 40, "password": "1234"}
    # params = json.dumps(newConditions).encode('utf8')

    req = urllib.request.Request('http://localhost:8081', data=params_as_json,
                                 headers={'content-type': 'application/json'})

    response = urllib.request.urlopen(req)
    print(response.read().decode('utf8'))


if __name__ == '__main__':
    p = {
        "cpu_pause_ms": 20,
        "message_bytes": 5000000,
        "period_sec": 0.03
    }
    set_new_params(p)
