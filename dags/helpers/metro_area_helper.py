from typing import Any
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook

import psycopg2
from psycopg2 import connect
import pymongo
from pymongo.collection import Collection

from data_models import MetroArea

class MetroAreaHelper:

    collection: Collection

    def __init__(self, mongo_client, mongo_database, mongo_collection):

        client = pymongo.MongoClient(mongo_client)
        database = client[mongo_database]
        self.collection = database[mongo_collection]

    def fetch_all_metro_areas(self) -> list[MetroArea]:

        all_metro_areas = list(self.collection.find())
        metro_areas = list(map(lambda m:MetroArea(m['country'], m['state'], m['city'], m['latitude'],
            m['longitude'], m['park_accessibility'], m['park_density'], m['robbery_index']), all_metro_areas))

        return metro_areas

class MetroAreaHelperV2:

    conn: Any #TODO: Adress typing

    def __init__(self):
        connection: Connection = BaseHook.get_connection("postgres_default")
        password = connection.password
        host = connection.host
        user = connection.login
        db = connection.extra_dejson
        print(db)
        self.conn = psycopg2.connect(dbname='airflow_db', user=user, password=password, host=host)

    def fetch_all_metro_areas(self) -> list[MetroArea]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT country, state, city, latitude, longitude, park_accessibility, park_density, robbery_index FROM MetroAreas")
        res = cursor.fetchall()
        x = map(lambda ele: MetroArea(*ele), res)
        cursor.close()
        return list(x)

