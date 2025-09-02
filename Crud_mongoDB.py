
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

Conection_String = ""

# Create a new client and connect to the server
client = MongoClient(Conection_String)
db_conection = client["Banco"]
collection = db_conection.get_collection("BancoColeção")



search_filter = {"ola" : "mundo"}
response = collection.find(search_filter)

for registry in response: print(registry)
