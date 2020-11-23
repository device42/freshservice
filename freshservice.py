# -*- coding: utf-8 -*-


import requests
from datetime import datetime
import time

requests.packages.urllib3.disable_warnings()


class FreshServiceBaseException(Exception):
    pass


class FreshServiceHTTPError(FreshServiceBaseException):
    pass


class FreshServiceDuplicateSerialError(FreshServiceHTTPError):
    pass


class FreshService(object):
    CITypeServerName = "Server"

    def __init__(self, endpoint, api_key, logger, **kwargs):
        self.base = endpoint
        self.api_key = api_key
        self.verify_cert = False
        self.debug = kwargs.get('debug', False)
        self.logger = logger
        self.base_url = "https://%s" % self.base
        self.headers = {}
        self.last_time_call_api = None
        self.period_call_api = 1
        self.api_call_count = 0
        self.asset_types = None

    def _send(self, method, path, data=None):
        """ General method to send requests """
        now = datetime.now()
        self.api_call_count += 1

        is_getting_exist = False
        if method == 'GET' and data is not None and "page" in data:
            is_getting_exist = True

        # if not is_getting_exist and self.last_time_call_api is not None and (
        #             now - self.last_time_call_api).total_seconds() < self.period_call_api:
        #     time.sleep(self.period_call_api - (now - self.last_time_call_api).total_seconds())

        url = "%s/%s" % (self.base_url, path)
        params = None
        if method == 'GET':
            params = data
            data = None

        while True:
            if method == 'GET':
                resp = requests.request(method, url, data=data, params=params,
                                        auth=(self.api_key, "X"),
                                        verify=self.verify_cert, headers=self.headers)
            else:
                resp = requests.request(method, url, json=data, params=params,
                                        auth=(self.api_key, "X"),
                                        verify=self.verify_cert, headers=self.headers)

            self.last_time_call_api = datetime.now()

            if not resp.ok:
                if resp.status_code == 429:
                    self._log("HTTP %s (%s) Error %s: %s\n request was %s" %
                              (method, path, resp.status_code, resp.text, data))
                    self._log("Throttling 1 min...")
                    time.sleep(60)
                    continue

                if resp.status_code == 400:
                    exception = None
                    try:
                        error_resp = resp.json()
                        if error_resp["description"] == "Validation failed":
                            for error in error_resp["errors"]:
                                if error["field"] == "serial_number" and error["message"] == " must be unique":
                                    exception = FreshServiceDuplicateSerialError("HTTP %s (%s) Error %s: %s\n request was %s" %
                                                                (method, path, resp.status_code, resp.text, data))
                                    break
                    except Exception:
                        pass

                    if exception is not None:
                        raise exception

                raise FreshServiceHTTPError("HTTP %s (%s) Error %s: %s\n request was %s" %
                                            (method, path, resp.status_code, resp.text, data))

            if method == "DELETE":
                return True

            if resp.status_code == 204:
                return {}

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

    def _delete(self, path, data=None):
        return self._send("DELETE", path, data)

    def _log(self, message, level="DEBUG"):
        if self.logger:
            self.logger.log(level.upper(), message)

    def insert_asset(self, data):
        path = "api/v2/assets"
        result = self._post(path, data)
        return result["asset"]["id"]

    def update_asset(self, data, display_id):
        path = "api/v2/assets/%d" % display_id
        result = self._put(path, data)
        return result["asset"]["id"]

    def delete_asset(self, display_id):
        path = "api/v2/assets/%d/delete_forever" % display_id
        result = self._put(path, {"No": 1})
        return result

    def get_assets_by_asset_type(self, asset_type_id):
        path = "api/v2/assets?include=type_fields&query=\"asset_type_id:%d\"" % asset_type_id
        assets = self._get(path)
        return assets["assets"]

    def insert_software(self, data):
        path = "api/v2/applications"
        result = self._post(path, data)
        return result["application"]["id"]

    def update_software(self, data, id):
        path = "api/v2/applications/%d" % id
        result = self._put(path, data)
        return result["application"]["id"]

    def delete_software(self, id):
        path = "api/v2/applications/%d" % id
        result = self._delete(path)
        return result

    def get_all_ci_types(self):
        if self.asset_types is not None:
            return self.asset_types
        path = "api/v2/asset_types"
        asset_types = self._get(path)
        self.asset_types = asset_types["asset_types"]
        return self.asset_types

    def get_ci_type_by_name(self, name, all_ci_types=None):
        if all_ci_types is None:
            all_ci_types = self.get_all_ci_types()

        for ci_type in all_ci_types:
            if ci_type["name"] == name:
                return ci_type

        return None

    def get_all_server_ci_types(self):
        all_ci_types = self.get_all_ci_types()

        server_types = []
        server_type = self.get_ci_type_by_name("Server", all_ci_types)
        if server_type is None:
            return []

        server_types.append(server_type)
        for ci_type in all_ci_types:
            if ci_type["parent_asset_type_id"] == server_type["id"]:
                server_types.append(ci_type)

        return server_types

    def get_server_ci_type(self):
        return self.get_ci_type_by_name(self.CITypeServerName)

    def get_windows_server_ci_type(self):
        return self.get_ci_type_by_name(self.CITypeWindowsServerName)

    def get_unix_server_ci_type(self):
        return self.get_ci_type_by_name(self.CITypeUnixServerName)

    def get_asset_type_fields(self, asset_type_id):
        path = "api/v2/asset_types/%d/fields" % asset_type_id
        return self._get(path)["asset_type_fields"]

    def get_all_server_assets(self):
        server_asset_types = self.get_all_server_ci_types()
        server_assets = []
        for asset_type in server_asset_types:
            assets = self.get_assets_by_asset_type(asset_type["id"])
            server_assets += assets

        return server_assets

    def get_products(self):
        path = "/api/v2/products"
        products = self._get(path)
        return products["products"]

    def get_vendors(self):
        path = "/api/v2/vendors"
        vendors = self._get(path)
        return vendors["vendors"]

    def get_id_by_name(self, model, name):
        path = "/api/v2/%s" % model
        models = self.request(path, "GET", model)
        for model in models:
            if "name" in model and model["name"] is not None and name is not None and \
                            model["name"].lower() == name.lower():
                return model["id"]

        return None

    def insert_and_get_id_by_name(self, model, name, asset_type_id):
        path = "/api/v2/%s" % model
        if asset_type_id is not None:
            data = {"name": name, "asset_type_id": asset_type_id}
        else:
            data = {"name": name}
        models = self._post(path, data)
        for key in models:
            return models[key]["id"]

        return None

    def request(self, source_url, method, model):
        if method == "GET":
            models = []
            page = 1
            while True:
                result = self._get(source_url, data={"page": page})
                if model in result:
                    models += result[model]
                    if len(result[model]) == 0:
                        break
                else:
                    break

                page += 1

            return models
        return []

    def get_relationship_type_by_content(self, forward, backward):
        path = "/cmdb/relationship_types/list.json"
        relationships = None
        if relationships is None:
            relationships = self._get(path)

        for relationship in relationships:
            if relationship["forward_relationship"] == forward and relationship["backward_relationship"] == backward:
                return relationship

        return None

    def get_relationships_by_id(self, asset_id):
        path = "/cmdb/items/%d/relationships.json" % asset_id
        result = self._get(path)
        if "relationships" in result:
            return result["relationships"]
        return []

    def insert_relationship(self, asset_id, data):
        path = "/cmdb/items/%d/associate.json" % asset_id
        relationships = self._post(path, data)
        if len(relationships) > 0:
            return relationships[0]["id"]

        return -1

    def detach_relationship(self, asset_id, relationship_id):
        path = "/cmdb/items/%d/detach_relationship.json" % asset_id

        return self._delete(path, {"relationship_id": relationship_id})

    def get_installations_by_id(self, display_id):
        path = "/api/v2/applications/%d/installations" % display_id

        models = []
        page = 1
        while True:
            result = self._get(path, data={"page": page})
            if "installations" in result:
                models += result["installations"]
                if len(result["installations"]) == 0:
                    break
            else:
                break

            page += 1

        return models

    def insert_installation(self, display_id, data):
        path = "/api/v2/applications/%d/installations" % display_id
        installation = self._post(path, data)
        if len(installation) > 0:
            return installation['installation']["id"]

        return -1
