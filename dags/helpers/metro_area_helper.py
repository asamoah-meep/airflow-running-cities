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