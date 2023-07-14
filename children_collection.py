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
collection = "childrens"
current_datetime = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
current_date = datetime.now().strftime("%d-%m-%Y")

session = boto3.session.Session(aws_access_key_id = "AKIAQBTIQ6VDCHHWNCNV", aws_secret_access_key = "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")
sm_client = session.client(service_name = "secretsmanager", region_name = region_name)


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


#Getting Children Data
childrens_df = pd.DataFrame()
childrens_cursor = db[collection].find({}, {"_id":1, "user":1, "name":1, "age":1, "gender":1, "accounts":1}, no_cursor_timeout = True)

for json_value in childrens_cursor:
    childrens_df = childrens_df.append(json_value, ignore_index = True)

childrens_df = childrens_df.explode("accounts")
childrens_df = childrens_df.reset_index(drop = True)


#Saving to s3 location
s3_client = session.client('s3')
s3_location = value["raw_location"]

bucket = s3_location.split("/")[2]
key = "/".join(s3_location.split("/")[3:]) + f"{current_date}/{collection}/{collection}_{current_datetime}.csv"

csv_string = childrens_df.to_csv(index = False)
s3_client.put_object(Bucket = bucket, Key = key, Body = csv_string)
print(f"Completed {collection} Collection, Saving file to:", "Bucket:", bucket, "Key:", key)


# Stop the tunnel
childrens_cursor.close()
mongo_client.close()
server.stop()
