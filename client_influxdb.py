import requests
import json


class InfluxDBClient:
    def __init__(self, host='localhost', port='8086', scheme='http', database=None):
        self._host = host
        self._port = int(port)
        self._scheme = scheme
        self._database = database

        self._headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'Accept': 'text/plain'
        }

        self._baseurl = "{0}://{1}:{2}".format(
            self._scheme,
            self._host,
            self._port
        )

    def request(self, url, method='GET', params=None, expected_response_code=200, headers=None):
        url = "{0}/{1}".format(self._baseurl, url)

        if params is None:
            params = {}

        if headers is None:
            headers = self._headers

        response = requests.request(
            url=url,
            method=method,
            data=params,
            headers=self._headers,

        )
        # TODO add exception
        if response.status_code == expected_response_code:
            return response

        else:
            raise requests.exceptions.ConnectionError

    def query(self, query, database=None, params=None, epoch=None,
              expected_response_code=200):
        if params is None:
            params = {}

        if epoch is not None:
            params['epoch'] = epoch

        result = []

        params['db'] = database or self._database

        for query_request in query:
            params['q'] = query_request
            response = self.request(
                url="query",
                method='POST',
                params=params,
                expected_response_code=expected_response_code
            )
            # TODO check null
            print response.text
            result.append(self.parse_json_response(response.text))
        return result

    def parse_json_response(self, response):
        # TODO {"results":[{"statement_id":0}]}
        # TODO {"results":[{"statement_id":0,"error":"unsupported call: m"}]}
        json_response = json.loads(response)
        values = []

        if 'error' in json_response['results']:
            raise Exception
        # TODO except if

        if 'series' in json_response['results']:
            values.append(json_response['results'][0]['series'][0]['values'][0][1])
            return values

        if 'series' in json_response['results']:
            variable_time_value = json_response['results'][0]['series'][0]['values']

            for key, value in variable_time_value:
                values.append(value)

        return values
