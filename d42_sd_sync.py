__author__ = 'Roman Nyschuk'

import os
import sys
import logging
import json
import argparse
import datetime
from device42 import Device42
from freshservice import FreshService, FreshServiceDuplicateSerialError
import xml.etree.ElementTree as eTree
from xmljson import badgerfish as bf
import time

logger = logging.getLogger('log')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(asctime)-15s\t%(levelname)s\t %(message)s'))
logger.addHandler(ch)
CUR_DIR = os.path.dirname(os.path.abspath(__file__))

parser = argparse.ArgumentParser(description="freshservice")

parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output')
parser.add_argument('-q', '--quiet', action='store_true', help='Quiet mode - outputs only errors')
parser.add_argument('-c', '--config', help='Config file', default='mapping.xml')
parser.add_argument('-l', '--logfolder', help='log folder path', default='.')

freshservice = None


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.strftime("%Y %m %d %H:%M:%S")
        return json.JSONEncoder.default(self, o)


def find_object_by_name(assets, name):
    for asset in assets:
        if asset["name"].lower() == name.lower():
            return asset

    return None


def get_asset_type_field(asset_type_fields, map_info):
    for section in asset_type_fields:
        if section["field_header"] == map_info["@target-header"]:
            for field in section["fields"]:
                name = map_info["@target"]
                if "@target-field" in map_info:
                    name = map_info["@target-field"]
                if field["asset_type_id"] is not None:
                    name += "_" + str(field["asset_type_id"])
                if field["name"] == name:
                    return field

    return None


def get_map_value_from_device42(source, map_info, b_add=False, asset_type_id=None):
    d42_value = source[map_info["@resource"]]
    if d42_value is None and "@resource-secondary" in map_info:
        d42_value = source[map_info["@resource-secondary"]]
    if "@is-array" in map_info and map_info["@is-array"]:
        d42_vals = d42_value
        d42_value = None
        for d42_val in d42_vals:
            if map_info["@sub-key"] in d42_val:
                d42_value = d42_val[map_info["@sub-key"]]
                break
    else:
        if "value-mapping" in map_info:
            d42_val = None
            if isinstance(map_info["value-mapping"]["item"], list):
                items = map_info["value-mapping"]["item"]
            else:
                items = [map_info["value-mapping"]["item"]]
            for item in items:
                if item["@key"] == d42_value:
                    d42_val = item["@value"]
            if d42_val is None and "@default" in map_info["value-mapping"]:
                default_value = map_info["value-mapping"]["@default"]

                # If we send a software status of "", we get the following error from the API:
                # Error 400: {"description":"Validation failed","errors":[{"field":"status",
                # "message":"It should be one of these values: 'blacklisted,ignored,managed'","code":"invalid_value"}]}
                # So if we have a value in D42 that does not map to Freshservice (we don't currently have a value that
                # does not map), instead of clearing the value in Freshservice by sending a "", it will try to set that
                # value and this is not one of the available options.  However, if we set the software status to None,
                # the value in Freshservice will get cleared.
                if default_value == "null":
                    d42_val = None
                else:
                    d42_val = default_value

            d42_value = d42_val
        else:
            pass

    if "@target-foregin-key" in map_info:
        value = freshservice.get_id_by_name(map_info["@target-foregin"], d42_value)
        if b_add and value is None and "@not-null" in map_info and map_info[
            "@not-null"]:  # and "@required" in map_info and map_info["@required"]
            if d42_value is not None:
                if "@max-length" in map_info and len(d42_value) > map_info["@max-length"]:
                    name = d42_value[0:map_info["@max-length"] - 3] + "..."
                else:
                    name = d42_value
                if map_info["@target-foregin"] == "vendors":
                    new_id = freshservice.insert_and_get_id_by_name(map_info["@target-foregin"], name, None)
                else:
                    new_id = freshservice.insert_and_get_id_by_name(map_info["@target-foregin"], name, asset_type_id)
                d42_value = new_id
            else:
                d42_value = None
        else:
            # If value is None, that means we could not find a match for the D42 value in Freshservice.
            # We will return the same D42 value since for product we will call this function again with
            # the required asset_type_id which is needed to create the value in Freshservice.
            if value is not None:
                d42_value = value

    return d42_value


def update_objects_from_server(sources, _target, mapping):
    global freshservice

    logger.info("Getting all existing devices in FS.")
    existing_objects = freshservice.request(_target["@path"] + "?include=type_fields", "GET", _target["@model"])
    logger.info("Finished getting all existing devices in FS.")

    asset_type = freshservice.get_ci_type_by_name(_target["@asset-type"])

    asset_type_fields = freshservice.get_asset_type_fields(asset_type["id"])

    for source in sources:
        error_skip = False
        while True:
            try:
                existing_object = find_object_by_name(existing_objects, source["name"])
                data = dict()
                data["type_fields"] = dict()
                for map_info in mapping["field"]:
                    if error_skip and "@error-skip" in map_info and map_info["@error-skip"]:
                        continue
                    asset_type_field = get_asset_type_field(asset_type_fields, map_info)
                    if asset_type_field is None:
                        continue

                    value = get_map_value_from_device42(source, map_info)

                    if asset_type_field["asset_type_id"] is not None:
                        data["type_fields"][asset_type_field["name"]] = value
                    else:
                        data[map_info["@target"]] = value

                # validation
                for map_info in mapping["field"]:
                    if error_skip and "@error-skip" in map_info and map_info["@error-skip"]:
                        continue

                    asset_type_field = get_asset_type_field(asset_type_fields, map_info)
                    if asset_type_field is None:
                        continue

                    if asset_type_field["asset_type_id"] is not None:
                        value = data["type_fields"][asset_type_field["name"]]
                    else:
                        value = data[map_info["@target"]]

                    is_valid = True
                    if value is not None and "@min-length" in map_info and len(value) < map_info["@min-length"]:
                        is_valid = False
                        if value == "" and "@set-space" in map_info and map_info["@set-space"]:
                            is_valid = True
                            value = " " * map_info["@min-length"]
                    # value might have been translated to an associated ID in Freshservice by get_map_value_from_device42
                    #  which is why we need to check that value is a string using isinstance.
                    if value is not None and "@max-length" in map_info and isinstance(value, str) and len(value) > map_info["@max-length"]:
                        value = value[0:map_info["@max-length"]-3] + "..."
                    if value is None and "@not-null" in map_info and map_info["@not-null"]:
                        if map_info["@target"] == "asset_tag":
                            is_valid = False
                        else:
                            # There is an issue with the Freshservice API where sending a null value for
                            # a field will result in the API returning an error like "Has 0 characters,
                            # it should have minimum of 1 characters and can have maximum of 255 characters".
                            # This prevents us from being able to clear these field values in Freshservice (even though
                            # the Freshservice UI allows you to clear these fields).  To get around this, we will send
                            # a single space for string values and a 0 for integer and float values when the value
                            # coming from D42 is null.
                            if "@target-type" in map_info:
                                target_type = map_info["@target-type"]
                                if target_type == "integer" or target_type == "float":
                                    value = 0
                                else:
                                    value = " "
                            else:
                                value = " "

                    if "@target-foregin-key" in map_info:
                        value = get_map_value_from_device42(source, map_info, True, data["asset_type_id"])
                        is_valid = value is not None
                    if "@target-type" in map_info and value is not None:
                        target_type = map_info["@target-type"]
                        if target_type == "integer":
                            try:
                                value = int(value)
                            except:
                                is_valid = False

                    if not is_valid:
                        logger.debug("argument '%s' is invalid." % map_info["@target"])
                        if asset_type_field["asset_type_id"] is not None:
                            data["type_fields"].pop(asset_type_field["name"], None)
                        else:
                            data.pop(map_info["@target"], None)
                    if is_valid:
                        if asset_type_field["asset_type_id"] is not None:
                            data["type_fields"][asset_type_field["name"]] = value
                        else:
                            data[map_info["@target"]] = value

                if existing_object is None:
                    logger.info("adding device %s" % source["name"])
                    new_asset_id = freshservice.insert_asset(data)
                    logger.info("added new asset %d" % new_asset_id)
                else:
                    logger.info("updating device %s" % source["name"])
                    updated_asset_id = freshservice.update_asset(data, existing_object["display_id"])
                    logger.info("updated new asset %d" % updated_asset_id)

                break
            except FreshServiceDuplicateSerialError:
                if not error_skip:
                    error_skip = True
                    continue
                break
            except Exception as e:
                log = "Error (%s) updating device %s" % (str(e), source["name"])
                logger.exception(log)
                break


def delete_objects_from_server(sources, _target, mapping):
    global freshservice

    logger.info("Getting all existing devices in FS.")
    existing_objects = freshservice.request(_target["@path"] + "?include=type_fields", "GET", _target["@model"])
    logger.info("finished getting all existing devices in FS.")

    for existing_object in existing_objects:
        exist = False
        for source in sources:
            if source[mapping["@key"]] == existing_object[mapping["@key"]]:
                exist = True
                break

        if not exist:
            try:
                logger.info("deleting device %s" % existing_object["name"])
                freshservice.delete_asset(existing_object["display_id"])
                logger.info("deleted asset %s" % existing_object["name"])
            except Exception as e:
                log = "Error (%s) deleting device %s" % (str(e), existing_object["name"])
                logger.exception(log)


def update_softwares_from_server(sources, _target, mapping):
    global freshservice

    logger.info("Getting all existing softwares in FS.")
    existing_objects = freshservice.request(_target["@path"], "GET", _target["@model"])
    logger.info("finished getting all existing softwares in FS.")

    for source in sources:
        try:
            existing_object = find_object_by_name(existing_objects, source["name"])
            data = dict()
            for map_info in mapping["field"]:
                value = get_map_value_from_device42(source, map_info)

                # value might have been translated to an associated ID in Freshservice by get_map_value_from_device42
                #  which is why we need to check that value is a string using isinstance.
                if value is not None and "@max-length" in map_info and isinstance(value, str) and len(value) > map_info["@max-length"]:
                    value = value[0:map_info["@max-length"] - 3] + "..."

                data[map_info["@target"]] = value

            if existing_object is None:
                logger.info("adding software %s" % source["name"])
                new_software_id = freshservice.insert_software(data)
                logger.info("added new software %d" % new_software_id)
            else:
                logger.info("updating software %s" % source["name"])
                updated_software_id = freshservice.update_software(data, existing_object["id"])
                logger.info("updated new software %d" % updated_software_id)
        except Exception as e:
            log = "Error (%s) updating software %s" % (str(e), source["name"])
            logger.exception(log)


def delete_softwares_from_server(sources, _target, mapping):
    global freshservice

    logger.info("Getting all existing softwares in FS.")
    existing_objects = freshservice.request(_target["@path"] + "?include=type_fields", "GET", _target["@model"])
    logger.info("finished getting all existing devices in FS.")

    for existing_object in existing_objects:
        exist = False
        for source in sources:
            if source[mapping["@key"]] == existing_object[mapping["@key"]]:
                exist = True
                break

        if not exist:
            try:
                logger.info("deleting software %s" % existing_object["name"])
                freshservice.delete_software(existing_object["id"])
                logger.info("deleted software %s" % existing_object["name"])
            except Exception as e:
                log = "Error (%s) deleting software %s" % (str(e), existing_object["name"])
                logger.exception(log)


def create_installation_from_software_in_use(sources, _target, mapping):
    global freshservice

    logger.info("Getting all existing devices in FS.")
    existing_objects = freshservice.request("api/v2/assets", "GET", "assets")
    logger.info("finished getting all existing devices in FS.")

    logger.info("Getting all existing softwares in FS.")
    existing_softwares = freshservice.request("api/v2/applications", "GET", "applications")
    logger.info("finished getting all existing softwares in FS.")

    for source in sources:
        try:
            logger.info("Processing %s - %s." % (source[mapping["@device-name"]], source[mapping["@software-name"]]))
            asset = find_object_by_name(existing_objects, source[mapping["@device-name"]])
            software = find_object_by_name(existing_softwares, source[mapping["@software-name"]])

            if asset is None:
                log = "There is no asset(%s) in FS." % source[mapping["@device-name"]]
                logger.exception(log)
                continue

            if software is None:
                log = "There is no software(%s) in FS." % source[mapping["@software-name"]]
                logger.exception(log)
                continue

            installations = freshservice.get_installations_by_id(software["id"])
            exist = False
            for installation in installations:
                if installation["installation_machine_id"] == asset["display_id"]:
                    exist = True
                    break
            if exist:
                logger.info("There is already installation in FS.")
                continue

            data = dict()
            data["installation_machine_id"] = asset["display_id"]
            data["version"] = source[mapping["@version"]]
            data["installation_date"] = source[mapping["@install-date"]]
            logger.info("adding installation %s-%s" % (source[mapping["@device-name"]], source[mapping["@software-name"]]))
            freshservice.insert_installation(software["id"], data)
            logger.info("added installation %s-%s" % (source[mapping["@device-name"]], source[mapping["@software-name"]]))
        except Exception as e:
            log = "Error (%s) creating installation %s" % (str(e), source[mapping["@device-name"]])
            logger.exception(log)


def create_relationships_from_affinity_group(sources, _target, mapping):
    global freshservice

    logger.info("Getting all existing devices in FS.")
    existing_objects = freshservice.request("api/v2/assets" + "?include=type_fields", "GET", _target["@model"])
    logger.info("finished getting all existing devices in FS.")

    logger.info("Getting relationship type in FS.")
    relationship_type = freshservice.get_relationship_type_by_content(mapping["@forward-relationship"],
                                                                      mapping["@backward-relationship"])
    logger.info("finished getting relationship type in FS.")
    if relationship_type is None:
        log = "There is no relationship type in FS. (%s - %s)" % (
            mapping["@forward-relationship"], mapping["@backward-relationship"])
        logger.info(log)
        return

    for source in sources:
        try:
            logger.info("Processing %s - %s." % (source[mapping["@key"]], source[mapping["@target-key"]]))
            primary_asset = find_object_by_name(existing_objects, source[mapping["@key"]])
            secondary_asset = find_object_by_name(existing_objects, source[mapping["@target-key"]])

            if primary_asset is None:
                log = "There is no dependent asset(%s) in FS." % source[mapping["@key"]]
                logger.exception(log)
                continue

            if secondary_asset is None:
                log = "There is no dependency asset(%s) in FS." % source[mapping["@target-key"]]
                logger.exception(log)
                continue

            relationships = freshservice.get_relationships_by_id(primary_asset["display_id"])
            exist = False
            for relationship in relationships:
                if relationship["relationship_type_id"] == relationship_type["id"]:
                    if relationship["config_item"]["display_id"] == secondary_asset["display_id"]:
                        exist = True
                        break
            if exist:
                logger.info("There is already relationship in FS.")
                continue

            data = dict()
            data["type"] = "config_items"
            data["type_id"] = [secondary_asset["display_id"]]
            data["relationship_type_id"] = relationship_type["id"]
            data["relationship_type"] = "forward_relationship"
            logger.info("adding relationship %s" % source[mapping["@key"]])
            new_relationship_id = freshservice.insert_relationship(primary_asset["display_id"], data)
            logger.info("added new relationship %d" % new_relationship_id)
        except Exception as e:
            log = "Error (%s) creating relationship %s" % (str(e), source[mapping["@key"]])
            logger.exception(log)


def delete_relationships_from_affinity_group(sources, _target, mapping):
    global freshservice

    logger.info("Getting all existing devices in FS.")
    existing_objects = freshservice.request("api/v2/assets" + "?include=type_fields", "GET", _target["@model"])
    logger.info("finished getting all existing devices in FS.")

    logger.info("Getting relationship type in FS.")
    relationship_type = freshservice.get_relationship_type_by_content(mapping["@forward-relationship"],
                                                                      mapping["@backward-relationship"])
    logger.info("finished getting relationship type in FS.")
    if relationship_type is None:
        log = "There is no relationship type in FS. (%s - %s)" % (
            mapping["@forward-relationship"], mapping["@backward-relationship"])
        logger.info(log)
        return

    for source in sources:
        try:
            logger.info("Processing %s - %s." % (source[mapping["@key"]], source[mapping["@target-key"]]))
            primary_asset = find_object_by_name(existing_objects, source[mapping["@key"]])
            secondary_asset = find_object_by_name(existing_objects, source[mapping["@target-key"]])

            if primary_asset is None:
                logger.info("There is no dependent asset(%s) in FS." % source[mapping["@key"]])
                continue

            if secondary_asset is None:
                logger.info("There is no dependency asset(%s) in FS." % source[mapping["@target-key"]])
                continue

            relationships = freshservice.get_relationships_by_id(primary_asset["display_id"])
            remove_relationship = None
            for relationship in relationships:
                if relationship["relationship_type_id"] == relationship_type["id"]:
                    if relationship["config_item"]["display_id"] == secondary_asset["display_id"]:
                        remove_relationship = relationship
                        break
            if remove_relationship is None:
                logger.info("There is no relationship in FS.")
                continue

            freshservice.detach_relationship(primary_asset["display_id"], remove_relationship["id"])
            logger.info("detached relationship %d" % remove_relationship["id"])
        except Exception as e:
            log = "Error (%s) deleting relationship %s" % (str(e), source[mapping["@key"]])
            logger.exception(log)


def create_relationships_from_business_app(sources, _target, mapping):
    create_relationships_from_affinity_group(sources, _target, mapping)


def delete_relationships_from_business_app(sources, _target, mapping):
    global freshservice

    logger.info("Getting all existing devices in FS.")
    existing_objects = freshservice.request("api/v2/assets" + "?include=type_fields", "GET", _target["@model"])
    logger.info("finished getting all existing devices in FS.")

    logger.info("Getting relationship type in FS.")
    relationship_type = freshservice.get_relationship_type_by_content(mapping["@forward-relationship"],
                                                                      mapping["@backward-relationship"])
    logger.info("finished getting relationship type in FS.")
    if relationship_type is None:
        log = "There is no relationship type in FS. (%s - %s)" % (
            mapping["@forward-relationship"], mapping["@backward-relationship"])
        logger.info(log)
        return

    for existing_object in existing_objects:
        try:
            logger.info("Checking relationship of asset(%s)." % existing_object["name"])
            relationships = freshservice.get_relationships_by_id(existing_object["display_id"])
            for relationship in relationships:
                if relationship["relationship_type_id"] == relationship_type["id"] and \
                                relationship["relationship_type"] == "forward_relationship":
                    remove_relationship = relationship
                    target_display_id = relationship["config_item"]["display_id"]
                    for source in sources:
                        if source[mapping["@key"]] == existing_object["name"]:
                            secondary_asset = find_object_by_name(existing_objects, source[mapping["@target-key"]])
                            if target_display_id == secondary_asset["display_id"]:
                                remove_relationship = None
                                break

                    if remove_relationship is None:
                        continue

                    freshservice.detach_relationship(existing_object["display_id"], remove_relationship["id"])
                    logger.info("detached relationship %d" % remove_relationship["id"])
        except Exception as e:
            log = "Error (%s) deleting relationship %s" % (str(e), existing_object[mapping["@key"]])
            logger.exception(log)


def parse_config(url):
    config = eTree.parse(url)
    meta = config.getroot()
    config_json = bf.data(meta)

    return config_json


def task_execute(task, device42):
    if "@description" in task:
        logger.info("Execute task - %s" % task["@description"])

    _resource = task["api"]["resource"]
    _target = task["api"]["target"]

    method = _resource['@method']
    if "@doql" in _resource:
        doql = _resource['@doql']
    else:
        doql = None

    source_url = _resource['@path']
    if "@extra-filter" in _resource:
        source_url += _resource["@extra-filter"] + "&amp;"

    _type = None
    if "@type" in task:
        _type = task["@type"]

    mapping = task['mapping']

    if doql is not None and doql:
        sources = device42.doql(source_url, method, query=doql)
    else:
        sources = device42.request(source_url, method, _resource["@model"])

    if _type == "affinity_group":
        if "@delete" in _target and _target["@delete"]:
            delete_relationships_from_affinity_group(sources, _target, mapping)
        else:
            create_relationships_from_affinity_group(sources, _target, mapping)
    elif _type == "business_app":
        if "@delete" in _target and _target["@delete"]:
            delete_relationships_from_business_app(sources, _target, mapping)
        else:
            create_relationships_from_business_app(sources, _target, mapping)
    elif _type == "software":
        if "@delete" in _target and _target["@delete"]:
            delete_softwares_from_server(sources, _target, mapping)
        else:
            update_softwares_from_server(sources, _target, mapping)
    elif _type == "software_in_use":
        if "@delete" in _target and _target["@delete"]:
            delete_softwares_from_server(sources, _target, mapping)
        else:
            create_installation_from_software_in_use(sources, _target, mapping)
    else:
        if "@delete" in _target and _target["@delete"]:
            delete_objects_from_server(sources, _target, mapping)
            return

        update_objects_from_server(sources, _target, mapping)


def main():
    global freshservice

    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    if args.quiet:
        logger.setLevel(logging.ERROR)

    try:
        log_file = "%s/d42_fs_sync_%d.log" % (args.logfolder, int(time.time()))
        logging.basicConfig(filename=log_file)
    except Exception as e:
        print("Error in config log: %s" % str(e))
        return -1

    config = parse_config(args.config)
    logger.debug("configuration info: %s" % (json.dumps(config)))

    settings = config["meta"]["settings"]
    device42 = Device42(settings['device42']['@url'], settings['device42']['@user'], settings['device42']['@pass'])
    freshservice = FreshService(settings['freshservice']['@url'], settings['freshservice']['@api_key'], logger)

    if not "task" in config["meta"]["tasks"]:
        logger.debug("No task")
        return 0

    if isinstance(config["meta"]["tasks"]["task"], list):
        tasks = config["meta"]["tasks"]["task"]
    else:
        tasks = [config["meta"]["tasks"]["task"]]

    for task in tasks:
        if not task["@enable"]:
            continue

        task_execute(task, device42)

    print("Completed! View log at %s" % log_file)
    return 0


if __name__ == "__main__":
    print('Running...')
    ret_val = main()
    print('Done')
    sys.exit(ret_val)
