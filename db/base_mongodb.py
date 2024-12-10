from config import settings
from pymongo import MongoClient

# MONGO_URI = f"mongodb://{settings.MONGODB_USER}:{settings.MONGODB_PASS}@{settings.MONGODB_HOST}:{settings.MONGODB_PORT}/?authMechanism=SCRAM-SHA-256&authSource={settings.MONGODB_NAME}"
MONGO_URI = f"mongodb://{settings.MONGODB_USER}:{settings.MONGODB_PASS}@{settings.MONGODB_HOST}:{settings.MONGODB_PORT}/{settings.MONGODB_NAME}?authSource=admin&retryWrites=true&w=majority"

def get_database():
    client = MongoClient(MONGO_URI)
    return client.get_database(settings.MONGODB_NAME)
