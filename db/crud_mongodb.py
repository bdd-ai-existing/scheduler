from db.base_mongodb import get_database

def upsert_bulk_insights(collection_name, bulk_data):
    """Perform bulk upsert operations for insights."""
    db = get_database()
    collection = db[collection_name]
    requests = [
        {
            "update_one": {
                "filter": {"account_id": item["account_id"], "date": item["date"]},
                "update": {"$set": item},
                "upsert": True
            }
        }
        for item in bulk_data
    ]
    if requests:
        collection.bulk_write(requests)

def insert_bulk_data(collection_name, bulk_data):
    """Perform bulk insert operations for insights."""
    db = get_database()
    collection = db[collection_name]
    collection.insert_many(bulk_data)
