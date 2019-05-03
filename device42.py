# -*- coding: utf-8 -*-


import os
import requests

requests.packages.urllib3.disable_warnings()


class Device42BaseException(Exception):
    pass


class Device42BadArgumentError(Exception):
    pass


class Device42HTTPError(Device42BaseException):
    pass


class Device42WrongRequest(Device42HTTPError):
    pass


class Device42(object):
    def __init__(self, endpoint, user, password, **kwargs):
        self.base = endpoint
        self.user = user
        self.pwd = password
        self.verify_cert = False
        self.debug = kwargs.get('debug', False)
        self.logger = kwargs.get('logger', None)
        self.base_url = "%s" % self.base
        self.headers = {}

    def _send(self, method, path, data=None):
        """ General method to send requests """
        url = "%s/%s" % (self.base_url, path)
        params = None
        if method == 'GET':
            params = data
            data = None
        resp = requests.request(method, url, data=data, params=params,
                                auth=(self.user, self.pwd),
                                verify=self.verify_cert, headers=self.headers)
        if not resp.ok:
            raise Device42HTTPError("HTTP %s (%s) Error %s: %s\n request was %s" %
                                    (method, path, resp.status_code, resp.text, data))
        retval = resp.json()
        return retval

    def _get(self, path, data=None):
        return self._send("GET", path, data=data)

    def _post(self, path, data):
        if not path.endswith('/'):
            path += '/'
        return self._send("POST", path, data=data)

    def _put(self, path, data):
        if not path.endswith('/'):
            path += '/'
        return self._send("PUT", path, data=data)

    def _delete(self, path):
        return self._send("DELETE", path)

    def _log(self, message, level="DEBUG"):
        if self.logger:
            self.logger.log(level.upper(), message)

    def get_device_by_name(self, name):
        path = "api/1.0/devices/name/%s" % name
        return self._get(path)

    def get_all_devices(self):
        path = "api/1.0/devices/all/"
        devices = []
        init_data = self._get(path, {'limit': 1, 'offset': 0})
        total_count = init_data['total_count']
        i = 0
        limit = 1000
        while i < total_count:
            devices_data = self._get(path, {'limit': limit, 'offset': i})
            devices = devices + devices_data['Devices']
            i += limit

        return devices

    def doql(self, url, method, query=None):
        path = url
        if query is None:
            query = "SELECT * FROM view_device_v1 order by device_pk"

        data = {"output_type": "json", "query": query}

        result = self._post(path, data)
        return result

    def request(self, source_url, method, model):
        models = []
        if method == "GET":
            result = self._get(source_url)
            if model in result:
                models = result[model]
            limit = 0
            total_count = 0
            if "limit" in result:
                limit = result["limit"]
            if "total_count" in result:
                total_count = result["total_count"]
            offset = limit
            while offset < total_count:
                result = self._get(source_url, data={"offset":offset, "limit":limit})
                if model in result:
                    models += result[model]
                offset += limit

        return models
