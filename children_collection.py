#Importing Libraries
import json
import boto3
import pymongo
import pandas as pd
from datetime import datetime


#Setting up Boto3 Client
region_name = "ap-southeast-2"
secret_name = "dashboard"
collection = "childrens"
current_datetime = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
current_date = datetime.now().strftime("%d-%m-%Y")

session = boto3.session.Session(region_name = region_name)
sm_client = session.client(service_name = "secretsmanager")
ec2_client = session.client(service_name = "ec2")


#Reading Data from Secrets Manager
try:
    get_secret_value_response = sm_client.get_secret_value(SecretId = secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
except Exception as e:
    print(e)


#Getting Public IP of EC2 server
ec2_response = ec2_client.describe_instances(InstanceIds=[value["ec2_instance_id"]])
ec2_ip = ec2_response["Reservations"][0]["Instances"][0].get("PublicIpAddress")


#Connecting to Mongodb
mongo_client = pymongo.MongoClient(f"""mongodb://{value["mongodb_user"]}:{value["mongodb_password"]}@{ec2_ip}:{value["mongodb_port"]}/{value["mongodb_database"]}""")
db = mongo_client[value["mongodb_database"]]

collection_names = db.list_collection_names()
print("Collections_Avaiable:", collection_names, "\n")


with mongo_client.start_session() as mongo_session:
    #Getting Children Data
    childrens_df = pd.DataFrame()
    childrens_cursor = db[collection].find({}, {"_id":1, "user":1, "name":1, "age":1, "gender":1, "accounts":1}, no_cursor_timeout = True, session = mongo_session)

    for json_value in childrens_cursor:
        childrens_df = pd.concat([childrens_df, pd.DataFrame([json_value])], ignore_index=True)

    childrens_df = childrens_df.explode("accounts")
    childrens_df = childrens_df.reset_index(drop = True)


#Saving to s3 location
s3_client = session.client("s3")
s3_location = value["raw_location"]

bucket = s3_location.split("/")[2]
key = "/".join(s3_location.split("/")[3:]) + f"{current_date}/{collection}/{collection}_{current_datetime}.csv"

csv_string = childrens_df.to_csv(index = False)
s3_client.put_object(Bucket = bucket, Key = key, Body = csv_string)
print(f"Completed {collection} Collection, Saving file to:", "Bucket:", bucket, "Key:", key)


#Close the Clients
childrens_cursor.close()
mongo_client.close()
