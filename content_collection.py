#Importing Libraries
import re
import json
import boto3
import pymongo
import pandas as pd
from datetime import datetime


#Setting up the Final Result
def final_result(x):
    if(isinstance(x, dict)):
        category_dict = {outer_key:sum(outer_value.values()) for outer_key, outer_value in x.items()}

        if(max(category_dict.values()) == 0):
            return "No"
        else:
            category = max(category_dict, key = category_dict.get)
            category = re.findall(r'[A-Z][^A-Z&]*|\&', category)
            return " ".join(category).title().strip()
    elif(str(x) in ["nan", "None"]):
        return "No"
    else:
        return str(x).title().strip()


#Setting up Boto3 Client
region_name = "ap-southeast-2"
secret_name = "dashboard"
collection = "contents"
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


#Getting Contents Data
contents_df = pd.DataFrame()
contents_cursor = db[collection].find({}, {"_id":1, "platform":1, "alert":1, "comments":1, "result":1, "createTime":1}, no_cursor_timeout = True)

for json_value in contents_cursor:
    contents_df = contents_df.append(json_value, ignore_index = True)

contents_df = contents_df.explode("comments")
contents_df["result_json"] = contents_df["result"]
contents_df["result"] = contents_df["result"].apply(final_result)
contents_df = contents_df.reset_index(drop = True)


#Saving to s3 location
s3_client = session.client("s3")
s3_location = value["raw_location"]

bucket = s3_location.split("/")[2]
key = "/".join(s3_location.split("/")[3:]) + f"{current_date}/{collection}/{collection}_{current_datetime}.csv"

csv_string = contents_df.to_csv(index = False)
s3_client.put_object(Bucket = bucket, Key = key, Body = csv_string)
print(f"Completed {collection} Collection, Saving file to:", "Bucket:", bucket, "Key:", key)


#Close the Clients
contents_cursor.close()
mongo_client.close()
