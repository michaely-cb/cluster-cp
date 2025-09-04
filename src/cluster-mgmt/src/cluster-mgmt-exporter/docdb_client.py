#!/usr/bin/env python
import argparse
import json
import os
import sys
from pymongo import MongoClient, ReadPreference
from typing import List

DEFAULT_SECRETS_FILE = "/cb/tools/cerebras/secrets.json"
DEFAULT_MONGO_HOST = "docdb.cerebras.aws"
DEFAULT_DOCDB_COLLECTION = "cluster_deployment"
DEFAULT_DOCDB_TABLE = "devices"
DEFAULT_DOCDB_USER = "cerebras"



def load_password(*args):
    override = os.environ.get("DM_DOCDB_PASSWORD")
    if override:
        return override

    try:
        with open(DEFAULT_SECRETS_FILE, 'r') as f:
            doc = json.loads(f.read())
    except Exception:
        raise ValueError(f"unable to read '{DEFAULT_SECRETS_FILE}', set envvar DM_DOCDB_PASSWORD if file not available")
    d = doc
    for arg in args:
        d = d.get(arg, {})
    return d.get("password", "")


def docdb_client(host, username, password, **kwargs) -> MongoClient:
    """ Default docdb connection """
    mongo_url = f"mongodb://{username}:{password}@{host}:27017"
    client_args = dict(
        replicaset="rs0",
        maxPoolSize=10,
        maxIdleTimeMS=60000,
        read_preference=ReadPreference.SECONDARY_PREFERRED,  # prefer reads on replica
    )
    client_args.update(kwargs)

    return MongoClient(mongo_url, **client_args)


class DeviceDBClient:
    def __init__(self, username=DEFAULT_DOCDB_USER, password=None):
        if not password:
            password = load_password("database", "docdb", username)
        self._client = docdb_client(DEFAULT_MONGO_HOST, username, password)
        self._collection = self._client[DEFAULT_DOCDB_COLLECTION][DEFAULT_DOCDB_TABLE]

    def list_devices(self, mongo_args: dict = None) -> List[dict]:
        """List devices with optional filtering based on mongo_args."""
        devices = self._collection.find(mongo_args or {})
        return [
            {**device, "_id": str(device["_id"])} if "_id" in device else device
            for device in devices
        ]

    def upsert_devices(self, devices: List[dict]):
        """Upsert devices based on the device name."""
        for device in devices:
            if "name" not in device:
                raise ValueError("Each device must have a 'name' field for upsert.")
            self._collection.update_one(
                {"name": device["name"]}, {"$set": device}, upsert=True
            )

### CLI for device maintenance
def main():
    parser = argparse.ArgumentParser(description="DeviceDB Client CLI")
    parser.add_argument("command", choices=["add", "list"], help="Command to execute")
    args = parser.parse_args()

    db_client = DeviceDBClient()

    if args.command == "list":
        devices = db_client.list_devices()
        print(json.dumps(devices, indent=2))

    elif args.command == "add":
        try:
            device_data = json.loads(sys.stdin.read())
            db_client.upsert_devices(device_data)
        except json.JSONDecodeError:
            print("Error: Invalid JSON in stdin")
            sys.exit(1)


if __name__ == "__main__":
    main()