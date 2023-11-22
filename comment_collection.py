#Importing Libraries
import re
import json
import boto3
import pymongo
import pandas as pd
from datetime import datetime
from bson import ObjectId


#Setting up the Final Result
def final_result(x):
    if(isinstance(x, dict)):
        category = max(x, key = x.get)
        category = re.findall('[a-z]+|[A-Z][a-z]*', category)
        return " ".join(category).title().strip()
    elif(str(x) in ["nan", "None"]):
        return "No"
    else:
        return str(x).title().strip()

    
#Setting up Boto3 Client
region_name = "ap-southeast-2"
secret_name = "dashboard"
collection = "comments"
current_datetime = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
current_date = datetime.now().strftime("%d-%m-%Y")

session = boto3.session.Session(region_name = region_name)
sm_client = session.client(service_name = "secretsmanager")
s3_client = session.resource("s3")
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


#Getting filter ids of comments
s3_filter_loc = value["filter_comments_location"]
filter_bucket_name = s3_filter_loc.split("/")[2]
filter_key = "/".join(s3_filter_loc.split("/")[3:]) + "filter.txt"
filtered_id_comments = s3_client.Object(filter_bucket_name, filter_key).get()['Body'].read().decode('utf-8').split("\n")
filtered_id_comments = [ObjectId(id) for id in filtered_id_comments]


#Getting Comments Data
comments_df = pd.DataFrame()

filtered_ids = { '_id': { '$in': filtered_id_comments } }
comments_cursor = db[collection].find(filtered_ids, {"_id":1, "platform":1, "alert":1, "commentTime":1, "result":1}, no_cursor_timeout = True)

for json_value in comments_cursor:
    comments_df = comments_df.append(json_value, ignore_index = True)

comments_df["result_json"] = comments_df["result"]
comments_df["result"] = comments_df["result"].apply(final_result)
comments_df = comments_df.reset_index(drop = True)


#Saving to s3 location
s3_client = session.client("s3")
s3_location = value["raw_location"]

bucket = s3_location.split("/")[2]
key = "/".join(s3_location.split("/")[3:]) + f"{current_date}/{collection}/{collection}_{current_datetime}.csv"

csv_string = comments_df.to_csv(index = False)
s3_client.put_object(Bucket = bucket, Key = key, Body = csv_string)
print(f"Completed {collection} Collection, Saving file to:", "Bucket:", bucket, "Key:", key)


#Close the Clients
comments_cursor.close()
mongo_client.close()
