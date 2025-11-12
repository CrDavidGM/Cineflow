"""Cliente MongoDB para CineFlow."""
from __future__ import annotations

from pymongo import MongoClient
from pymongo.database import Database

from cineflow.utils.config import settings


def get_mongo() -> tuple[MongoClient, Database]:
    """Devuelve un cliente MongoDB y su base de datos configurada."""
    client: MongoClient = MongoClient(settings.MONGO_URI)
    db: Database = client.get_database(settings.MONGO_DB)
    return client, db
