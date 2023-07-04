# -*- coding: utf-8 -*-


import requests
from datetime import datetime
import time
import logging
import jwt
import pytz

requests.packages.urllib3.disable_warnings()

# in seconds
DEFAULT_RETRY_AFTER = 10
RETRY_AFTER_HEADER = 'Retry-After'


class FreshServiceBaseException(Exception):
    pass


class FreshServiceHTTPError(FreshServiceBaseException):
    pass


class FreshServiceDuplicateValueError(FreshServiceHTTPError):
    pass


class FreshService(object):
    CITypeServerName = "Server"
    PAGE_SIZE = 100
    FS_INTEGRATION_NAME_HEADER = 'FS-INTEGRATION-NAME'
    JWT_ALGORITHM = 'HS256'
    JWT_RECREATE_TIME = 15

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
        self.created_by_jwt = None
        self.expired_time_jwt = None

    def _get_created_by_jwt(self):
        if self.created_by_jwt is not None:
            if self.expired_time_jwt - int(time.time()) > self.JWT_RECREATE_TIME:
                return self.created_by_jwt

        timestamp = int(round(time.time() + 120))
        encoded = jwt.encode(
            {'iss': 'device_42', 'exp': timestamp},
            self.api_key,
            self.JWT_ALGORITHM
        )

        self.created_by_jwt = encoded
        self.expired_time_jwt = timestamp
        return encoded

    def _send(self, method, path, data=None, headers=None):
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

        all_headers = self.headers
        if headers:
            all_headers.update(headers)

        while True:
            if method == 'GET':
                resp = requests.request(method, url, data=data, params=params,
                                        auth=(self.api_key, "X"),
                                        verify=self.verify_cert, headers=all_headers)
            else:
                resp = requests.request(method, url, json=data, params=params,
                                        auth=(self.api_key, "X"),
                                        verify=self.verify_cert, headers=all_headers)

            self.last_time_call_api = datetime.now()

            if not resp.ok:
                if resp.status_code == 429:
                    self._log("HTTP %s (%s) Error %s: %s\n request was %s" %
                              (method, path, resp.status_code, resp.text, data))

                    retry_after = DEFAULT_RETRY_AFTER
                    header_value = resp.headers.get(RETRY_AFTER_HEADER)
                    if header_value:
                        try:
                            retry_after = int(header_value)
                        except ValueError as e:
                            client.captureException(extra={'header_value': header_value})
                            self._log('Failed to convert Retry-After value of "%s" to int: %s' % (header_value, str(e)))

                    self._log("Throttling %d second(s)..." % retry_after)
                    time.sleep(retry_after)
                    continue

                if resp.status_code == 400:
                    exception = None
                    try:
                        error_resp = resp.json()
                        if error_resp["description"] == "Validation failed":
                            for error in error_resp["errors"]:
                                if (error["field"] == "serial_number" or error["field"] == "item_id") and \
                                        (error["message"] == " must be unique" or error["message"] == " is not unique"):
                                    exception = FreshServiceDuplicateValueError("HTTP %s (%s) Error %s: %s\n request was %s" %
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

    def _post(self, path, data, headers=None):
        if not path.endswith('/'):
            path += '/'
        return self._send("POST", path, data=data, headers=headers)

    def _put(self, path, data, headers=None):
        if not path.endswith('/'):
            path += '/'
        return self._send("PUT", path, data=data, headers=headers)

    def _delete(self, path, data=None):
        return self._send("DELETE", path, data)

    def _log(self, message, level=logging.DEBUG):
        if self.logger:
            self.logger.log(level, message)

    def _get_asset_headers(self):
        return {self.FS_INTEGRATION_NAME_HEADER: self._get_created_by_jwt()}

    def insert_asset(self, data):
        path = "api/v2/assets"
        result = self._post(path, data, self._get_asset_headers())
        return self.create_basic_object(result["asset"])

    def update_asset(self, data, display_id):
        path = "api/v2/assets/%d" % display_id
        result = self._put(path, data, self._get_asset_headers())
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
        return self.create_basic_object(result["application"])

    def update_software(self, data, id):
        path = "api/v2/applications/%d" % id
        result = self._put(path, data)
        return result["application"]["id"]

    def delete_software(self, id):
        path = "api/v2/applications/%d" % id
        result = self._delete(path)
        return result

    def insert_product(self, data):
        path = "api/v2/products"
        result = self._post(path, data)
        return self.create_basic_object(result["product"])

    def update_product(self, data, id):
        path = "api/v2/products/%d" % id
        result = self._put(path, data)
        return result["product"]["id"]

    def insert_contract(self, data):
        path = "api/v2/contracts"
        result = self._post(path, data)
        return self.create_basic_object(result["contract"])

    def update_contract(self, data, id):
        path = "api/v2/contracts/%d" % id
        result = self._put(path, data)
        return result["contract"]["id"]

    def get_associated_assets_by_contract(self, contract_id):
        path = "/api/v2/contracts/%d/associated-assets" % contract_id
        return self.request(path, "GET", "associated_assets")

    def get_all_ci_types(self):
        if self.asset_types is not None:
            return self.asset_types
        path = "api/v2/asset_types"
        self.asset_types = self.request(path, "GET", "asset_types")
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

    def get_all_agents(self):
        path = "/api/v2/agents"
        return self.request(path, "GET", "agents")

    def get_agents(self, search, page, per_page):
        path = "/api/v2/agents"
        data = {'page': page, 'per_page': per_page}
        if search and len(search) >= 2:
            data['query'] = '"~[name|first_name|last_name|email]:\'' + search + '\'"'
        vendors = self._get(path, data)
        return vendors["agents"]

    def get_id_by_name(self, model, name, foregin_key="name"):
        path = "/api/v2/%s" % model
        models = self.request(path, "GET", model)
        for model in models:
            if foregin_key in model and model[foregin_key] is not None and name is not None and \
                            model[foregin_key].lower() == name.lower():
                return model["id"]

        return None

    def insert_and_get_by_name(self, model, name, asset_type_id, foregin_key="name"):
        path = "/api/v2/%s" % model
        if asset_type_id is not None:
            data = {foregin_key: name, "asset_type_id": asset_type_id}
        else:
            data = {foregin_key: name}
        models = self._post(path, data)
        for key in models:
            return self.create_basic_object(models[key])

        return None

    def request(self, source_url, method, model):
        if method == "GET":
            models = []
            page = 1
            while True:
                result = self._get(source_url, data={"page": page, "per_page": self.PAGE_SIZE})
                if model in result:
                    models += result[model]
                    if len(result[model]) == 0:
                        break
                else:
                    break

                page += 1

            return models
        return []

    def normalize_value(self, val):
        if val:
            # Replace Unicode no-break spaces with normal spaces.
            # We are doing the same with the data we get from D42.
            # This will allow us to find objects in Freshservice regardless if the name
            # is using Unicode no-break spaces or normal ASCII spaces since the Unicode
            # no-break spaces will always be converted to normal ASCII spaces.
            return val.replace(u'\xa0', ' ')

        return val

    def create_basic_object(self, m):
        # Create an object using only the properties that we will need.  This object will
        # be stored in the cache, so we want to try to minimize the memory footprint of it.
        obj = {"id": m["id"]}

        if "name" in m:
            obj["name"] = self.normalize_value(m["name"])

        if "email" in m:
            obj["email"] = m["email"]

        # Not all models have a display_id (e.g. assets do, but asset types do not).
        if "display_id" in m:
            obj["display_id"] = m["display_id"]

        if "agent_id" in m:
            obj["agent_id"] = m["agent_id"]

        if "asset_type_id" in m:
            obj["asset_type_id"] = m["asset_type_id"]

        return obj

    def get_objects_map(self, source_url, model, foregin_key="name"):
        objects = self.request(source_url, "GET", model)
        # Return a dictionary where the key is the lowercase name of the object and the value is
        # the basic object (e.g. id, name, etc.).
        return {self.normalize_value(obj[foregin_key]).lower(): self.create_basic_object(obj) for obj in objects}

    def get_relationship_type_by_content(self, downstream, upstream):
        path = "/api/v2/relationship_types"
        relationship_types = self.request(path, "GET", "relationship_types")

        for relationship_type in relationship_types:
            if relationship_type["downstream_relation"] == downstream and relationship_type["upstream_relation"] == upstream:
                return relationship_type

        return None

    def get_relationships_by_id(self, asset_id):
        path = "/api/v2/assets/%d/relationships" % asset_id
        return self.request(path, "GET", "relationships")

    def insert_relationships(self, data):
        path = "/api/v2/relationships/bulk-create"
        job = self._post(path, data)
        return job["job_id"]

    def detach_relationship(self, relationship_id):
        path = "/api/v2/relationships?ids=%d" % relationship_id
        return self._delete(path)

    def get_installations_by_id(self, display_id):
        path = "/api/v2/applications/%d/installations" % display_id
        return self.request(path, "GET", "installations")

    def insert_installation(self, display_id, data):
        path = "/api/v2/applications/%d/installations" % display_id
        installation = self._post(path, data)
        if len(installation) > 0:
            return installation['installation']["id"]

        return -1

    def get_job(self, job_id):
        path = "/api/v2/jobs/%s" % job_id
        return self._get(path)
