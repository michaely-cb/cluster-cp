import logging
import re
from typing import List
from typing import Optional
from typing import Sequence

from pymongo import MongoClient
from pymongo.collection import Collection

logger = logging.getLogger("cs_cluster.common.devinfra")


class DevInfraDB:
    """ Helper to access the devinfra database.
    WebLink: http://systems-db.devinfra.cerebras.aws/view/multiboxes
    """

    DEFAULT_URL = "mongodb://csUser:csUser@ua.cerebras.aws:27017/?authSource=cs1&authMechanism=SCRAM-SHA-1"
    DEFAULT_DB = "cs1"
    DEFAULT_COLLECTION = "multiboxes"

    def __init__(self, url: str = ""):
        self._url = DevInfraDB.DEFAULT_URL if not url else url
        self._client = None

    def __enter__(self):
        self._client = MongoClient(self._url)
        return self

    def __exit__(self, type, value, traceback):
        self._client.close()
        self._client = None

    def _get_collection(self) -> Collection:
        if not self._client:
            raise RuntimeError("DevinfraDB not opened")
        return self._client[DevInfraDB.DEFAULT_DB][DevInfraDB.DEFAULT_COLLECTION]

    def get_cluster(self, name: str) -> Optional[dict]:
        collection = self._get_collection()
        docs = collection.find({"name": name}).limit(1)
        try:
            return next(docs)
        except StopIteration:
            return None

    def list_usernodes(self, cluster_name: str) -> List[str]:
        doc = self.get_cluster(cluster_name)
        if doc:
            return [re.sub(r"\.cerebrassc\.local$", "", h) for h in doc.get("user_hosts", [])]
        return []

    def list_cluster_names(self) -> Sequence[str]:
        collection = self._get_collection()
        return [doc['name'] for doc in collection.find({}, {'name': 1, "_id": 0}).sort("name")]
