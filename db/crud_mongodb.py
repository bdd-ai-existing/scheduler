from db.base_mongodb import get_database

def upsert_bulk_insights(collection_name, requests):
    """Perform bulk upsert operations for insights."""
    db = get_database()
    collection = db[collection_name]

    if requests:
        collection.bulk_write(requests)

def insert_bulk_data(collection_name, bulk_data):
    """Perform bulk insert operations for insights."""
    db = get_database()
    collection = db[collection_name]
    collection.insert_many(bulk_data)

"""
get all the data from mongodb using filter
"""
def get_data(collection_name, filter, projection=None):
    db = get_database()
    collection = db[collection_name]
    return collection.find(filter, projection)

"""
delete all the data from mongodb using filter
"""
def delete_data(collection_name, filter):
    db = get_database()
    collection = db[collection_name]
    collection.delete_many(filter)

"""
update one data from mongodb using filter
"""
def update_data(collection_name, filter, data):
    db = get_database()
    collection = db[collection_name]
    collection.update_one(filter, data)