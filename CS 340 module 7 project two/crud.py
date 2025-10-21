"""
crud.py
Reusable CRUD class for MongoDB using PyMongo for CS 340 Project One (Grazioso Salvare).

Author: Lasupe Xiong
Notes:
- Designed for reuse from other Python modules and Jupyter notebooks.
- Uses robust exception handling and logging-style messages.
- Follows Python style guidelines: clear naming, docstrings, and inline comments.
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from pymongo import MongoClient, errors
from pymongo.collection import Collection


@dataclass
class MongoConfig:
    """
    Configuration bundle for MongoDB connectivity.
    """
    username: str = ""
    password: str = ""
    host: str = "localhost"
    port: int = 27017
    authSource: str = "aac"           # Database where the user is created
    tls: bool = False                 # Set to True if using TLS/SSL
    replicaSet: Optional[str] = None  # Optional replica set name


class CRUD:
    """
    CRUD helper for MongoDB collections.
    Initialize once, then call create/read/update/delete methods.

    Example:
        cfg = MongoConfig(username="aacuser", password="***")
        crud = CRUD(cfg, db_name="aac", collection_name="animals")
        crud.create({"name": "Fido"})
    """

    def __init__(self,
                 config: MongoConfig,
                 db_name: str,
                 collection_name: str) -> None:
        """
        Establish a connection to MongoDB and select the collection.
        """
        self._client: Optional[MongoClient] = None
        self._collection: Optional[Collection] = None
        self._db_name = db_name
        self._collection_name = collection_name
        self._connect(config)

    # --------------------------- Internal Helpers --------------------------- #

    def _connect(self, config: MongoConfig) -> None:
        """
        Build a MongoClient from the provided config and set the collection.
        """
        try:
            client_kwargs = {
                "host": config.host,
                "port": config.port,
                "tls": config.tls,
                "connectTimeoutMS": 5000,
                "serverSelectionTimeoutMS": 5000,
            }
            # Only include credentials if provided
            if config.username and config.password:
                client_kwargs.update({
                    "username": config.username,
                    "password": config.password,
                    "authSource": config.authSource,
                })
            if config.replicaSet:
                client_kwargs["replicaSet"] = config.replicaSet

            self._client = MongoClient(**client_kwargs)
            # Force a ping so connection errors surface immediately.
            self._client.admin.command("ping")

            db = self._client[self._db_name]
            self._collection = db[self._collection_name]
        except errors.PyMongoError as e:
            raise RuntimeError(f"[MongoDB] Connection failed: {e}") from e

    def _ensure_collection(self) -> Collection:
        """
        Ensure the collection object is initialized.
        """
        if self._collection is None:
            raise RuntimeError("[MongoDB] Collection is not initialized.")
        return self._collection

    # --------------------------- Public Operations -------------------------- #

    def create(self, document: Dict[str, Any]) -> bool:
        """
        Insert a single document into the collection.
        Returns True on success, False otherwise.
        """
        try:
            if not isinstance(document, dict):
                raise ValueError("create() expects a dict document.")
            result = self._ensure_collection().insert_one(document)
            return result.acknowledged and result.inserted_id is not None
        except errors.PyMongoError as e:
            print(f"[MongoDB][CREATE] Error: {e}")
            return False

    def read(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Find documents matching 'query' using find().
        Returns a list of documents; returns an empty list on error.
        """
        try:
            if not isinstance(query, dict):
                raise ValueError("read() expects a dict query.")
            cursor = self._ensure_collection().find(query)
            return list(cursor)
        except errors.PyMongoError as e:
            print(f"[MongoDB][READ] Error: {e}")
            return []

    def update(self,
               query: Dict[str, Any],
               new_values: Dict[str, Any],
               many: bool = False) -> int:
        """
        Update one or many documents that match 'query'.
        'new_values' should be a complete update spec (e.g., {'$set': {...}}).
        Set many=True to use update_many(); otherwise update_one() is used.
        Returns the number of modified documents.
        """
        try:
            if not isinstance(query, dict):
                raise ValueError("update() expects a dict query.")
            if not isinstance(new_values, dict):
                raise ValueError("update() expects a dict update spec (e.g., {'$set': {...}}).")

            coll = self._ensure_collection()
            if many:
                result = coll.update_many(query, new_values)
            else:
                result = coll.update_one(query, new_values)

            return int(result.modified_count or 0)
        except errors.PyMongoError as e:
            print(f"[MongoDB][UPDATE] Error: {e}")
            return 0

    def delete(self, query: Dict[str, Any], many: bool = False) -> int:
        """
        Delete one or many documents that match 'query'.
        Set many=True to use delete_many(); otherwise delete_one() is used.
        Returns the number of deleted documents.
        """
        try:
            if not isinstance(query, dict):
                raise ValueError("delete() expects a dict query.")
            coll = self._ensure_collection()
            if many:
                result = coll.delete_many(query)
            else:
                result = coll.delete_one(query)
            return int(result.deleted_count or 0)
        except errors.PyMongoError as e:
            print(f"[MongoDB][DELETE] Error: {e}")
            return 0
