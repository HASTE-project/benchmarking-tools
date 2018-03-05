import urllib.request
import json
import time


class SparkMonitor:

    def __init__(self, host, port=4040):
        self.host = host
        self.port = port

    def start(self):
        app = self.__do_request_get_applications(self.host, self.port)
        self.app_id = app[0]['id']

    @staticmethod
    def __do_request_get_applications(host, port):
        req = urllib.request.Request("http://%s:%d/api/v1/applications" % (host, port),
                                     headers={'Content-type': 'application/json',
                                              'Accept': 'application/json'})

        f = urllib.request.urlopen(req)
        parsed = json.loads(f.read())
        # e.g.: [
        #     {
        #         "id": "local-1519971222469",
        #         "name": "StreamingBenchmark",
        #         "attempts": [
        #             {
        #                 "startTime": "2018-03-02T06:13:41.287GMT",
        #                 "endTime": "1969-12-31T23:59:59.999GMT",
        #                 "lastUpdated": "2018-03-02T06:13:41.287GMT",
        #                 "duration": 0,
        #                 "sparkUser": "benblamey",
        #                 "completed": false,
        #                 "startTimeEpoch": 1519971221287,
        #                 "endTimeEpoch": -1,
        #                 "lastUpdatedEpoch": 1519971221287
        #             }
        #         ]
        #     }
        # ]
        return parsed

    @staticmethod
    def __do_request_GET_streaming_statistics(host, port, app_id):
        url = "http://%s:%d/api/v1/applications/%s/streaming/statistics" % (host, port, app_id)
        #print(url)
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

    def get_status(self):
        stats = self.__do_request_GET_streaming_statistics(self.host, self.port, self.app_id)
        return stats


if __name__ == '__main__':
    monitor = SparkMonitor('localhost', port=4040)
    monitor.start()
    while True:
        stats = monitor.get_status()
        time.sleep(1)
        #print(stats)

        # Don't use the average -- we want the latest!!!
        print(stats['avgTotalDelay'])
    #
    # parsed = __do_request_GET_applications('localhost', 4040)
    # app_id = parsed[0]['id']
    # print(app_id)
    #
    # stats = __do_request_GET_streaming_statistics('localhost', 4040, app_id)

    # avg_total_delay = stats['avgTotalDelay']


