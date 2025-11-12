from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI","mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "etl_demo")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

schemas_col = db["schemas"]
batches_col = db["batches"]
records_col = db["records"]

