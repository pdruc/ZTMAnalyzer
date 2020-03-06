import ast
import time
import requests
import configparser

cnf = configparser.ConfigParser()
cnf.read('configuration.cfg')


def print_when_request_finished(status_code, status_reason, elapsed_time):
    print(f'Request finished in {elapsed_time:.2f} with code {status_code}: {status_reason}.')


class APIClient:
    api_url = cnf.get('resources', 'api_url')
    resource_id = cnf.get('credentials', 'resource_id')
    api_key = cnf.get('credentials', 'api_key')

    def __init__(self, bus_or_tram='bus'):
        self.bus_or_tram = bus_or_tram
        self.type = 1 if self.bus_or_tram == 'bus' else 2
        self.parameters_dict = {'resource_id': self.resource_id, 'apikey': self.api_key, 'type': self.type}
        self.parameters_str = self._parse_parameters()

        self.url = self.api_url + self.parameters_str
        self.payload = dict()
        self.headers = dict()

        self.response = None
        self.data = None

    def _parse_parameters(self):
        return '/?' + '&'.join(['{0}={1}'.format(k, v) for k, v in self.parameters_dict.items()])

    def get_data(self):
        # Make request and print info
        time_start = time.time()
        self.response = requests.request('GET', self.url, headers=self.headers, data=self.payload)
        print_when_request_finished(self.response.status_code, self.response.reason, time.time() - time_start)


if __name__ == '__main__':
    pass


