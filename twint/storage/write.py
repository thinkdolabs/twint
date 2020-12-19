from . import write_meta as meta
import csv
from datetime import datetime
import json
import logging
import os

from arecibo import http
from arecibo.ingest import ingest, track_event


def outputExt(objType, fType):
    if objType == "str":
        objType = "username"
    outExt = f"/{objType}s.{fType}"

    return outExt

def addExt(base, objType, fType):
    if len(base.split('.')) == 1:
        createDirIfMissing(base)
        base += outputExt(objType, fType)

    return base

def Text(entry, f):
    print(entry.replace('\n', ' '), file=open(f, "a", encoding="utf-8"))

def Type(config):
    if config.User_full:
        _type = "user"
    elif config.Followers or config.Following:
        _type = "username"
    else:
        _type = "tweet"

    return _type

def struct(obj, custom, _type):
    if custom:
        fieldnames = custom
        row = {}
        for f in fieldnames:
            row[f] = meta.Data(obj, _type)[f]
    else:
        fieldnames = meta.Fieldnames(_type)
        row = meta.Data(obj, _type)

    return fieldnames, row

def createDirIfMissing(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)

def Csv(obj, config):
    _obj_type = obj.__class__.__name__
    if _obj_type == "str":
        _obj_type = "username"
    fieldnames, row = struct(obj, config.Custom[_obj_type], _obj_type)
    
    base = addExt(config.Output, _obj_type, "csv")
    dialect = 'excel-tab' if 'Tabs' in config.__dict__ else 'excel'
    
    if not (os.path.exists(base)):
        with open(base, "w", newline='', encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames, dialect=dialect)
            writer.writeheader()

    with open(base, "a", newline='', encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, dialect=dialect)
        writer.writerow(row)

def Json(obj, config):
    _obj_type = obj.__class__.__name__
    if _obj_type == "str":
        _obj_type = "username"
    null, data = struct(obj, config.Custom[_obj_type], _obj_type)

    base = addExt(config.Output, _obj_type, "json")

    with open(base, "a", newline='', encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False)
        json_file.write("\n")

async def Webhook(data, config):
    """Send an http webhook: { timestamp, data, metadata }"""
    obj = data[0]
    _obj_type = obj.__class__.__name__
    if _obj_type == "str":
        _obj_type = "username"
    all_data = []
    metadata = config.WebhookMetadata
    for obj in data:
        null, data = struct(obj, config.Custom[_obj_type], _obj_type)
        all_data.append(data)
    response = ingest('tweet', all_data, metadata)

    event_name = 'twint-webhook-request'
    event_data = {
        'url': config.Webhook,
        'status': response['status'],
        'num_records': len(all_data) if isinstance(all_data, list) else 1,
    }
    track_event(event_name, event_data)




