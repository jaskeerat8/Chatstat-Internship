#Importing Libraries
import json
import boto3
import pymongo
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from datetime import datetime

#Setting up Boto3 Client
region_name = "ap-southeast-2"
secret_name = "dashboard"
collection = "users"
current_datetime = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
current_date = datetime.now().strftime("%d-%m-%Y")

session = boto3.session.Session(region_name = region_name)
sm_client = session.client(service_name = "secretsmanager")


#Reading Data from Secrets Manager
try:
    get_secret_value_response = sm_client.get_secret_value(SecretId = secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
except Exception as e:
    print(e)

remote_server_ip = value["remote_server_ip"]
remote_server_username = value["remote_server_username"]
remote_server_password = value["remote_server_password"]

server = SSHTunnelForwarder(
    ssh_address_or_host = (remote_server_ip, 22),
    ssh_username = remote_server_username,
    ssh_password = remote_server_password,
    remote_bind_address = ('localhost', 9017),
    local_bind_address = ('localhost', 9017),
    ssh_pkey = None
)
server.start()

mongo_client = pymongo.MongoClient(f"""mongodb://{value["mongodb_user"]}:{value["mongodb_password"]}@localhost:9017/{value["mongodb_database"]}""")
db = mongo_client[value["mongodb_database"]]

collection_names = db.list_collection_names()
print("Collections_Avaiable:", collection_names, "\n")


#Getting User Data
users_df = pd.DataFrame()
users_cursor = db[collection].find({}, {"_id":1, "children":1, "name":1, "email":1, "role":1}, no_cursor_timeout = True)

for json_value in users_cursor:
    users_df = users_df.append(json_value, ignore_index = True)

users_df = users_df.explode("children")
users_df = users_df.reset_index(drop = True)


#Saving to s3 location
s3_client = session.client('s3')
s3_location = value["raw_location"]

bucket = s3_location.split("/")[2]
key = "/".join(s3_location.split("/")[3:]) + f"{current_date}/{collection}/{collection}_{current_datetime}.csv"

csv_string = users_df.to_csv(index = False)
s3_client.put_object(Bucket = bucket, Key = key, Body = csv_string)
print(f"Completed {collection} Collection, Saving file to:", "Bucket:", bucket, "Key:", key)


# Stop the tunnel
users_cursor.close()
mongo_client.close()
server.stop()
