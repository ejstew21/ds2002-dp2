from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.ryb8jt
collection = db.dp2

directory = 'data'

files_imported = 0
failed_load = 0
for file in os.listdir(directory):
    with open(os.path.join(directory, file)) as f:
        try:
            file_data = json.load(f)
            files_imported +=1
        except Exception as e:
            failed_load +=1
            print(e, "error when loading", f)
        if isinstance(file_data,list):
            try:
                collection.insert_many(file_data)
                files_imported += len(file_data)
            except Exception as e:
                failed_load+=1
                print(e, "when importing into Mongo")
        else:
            try:
                collection.insert_one(file_data)
            except Exception as e:
                print(e)
                failed_load +=1
print(files_imported, failed_load)
