#Importing Libraries
import io
import json
import boto3
import pymongo
from datetime import datetime, timedelta

def lambda_handler(event, context):
    #Setting up Boto3 Client
    region_name = "ap-southeast-2"
    secret_name = "dashboard"
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
    try:
        mongo_client = pymongo.MongoClient(f"""mongodb://{value["mongodb_user"]}:{value["mongodb_password"]}@{ec2_ip}:{value["mongodb_port"]}/{value["mongodb_database"]}""")
        db = mongo_client[value["mongodb_database"]]
        collection_names = db.list_collection_names()
        print("MongoDB connected. Collections_Avaiable:", collection_names, "\n")
    except Exception as e:
        print("Error Connecting to MongoDB")
    
    
    #Getting Users Children Data
    users_collection = db["users"]
    users_pipeline = [
        {"$match": {"name": event["name"], "email": event["email"]}},
        {"$project": {"children": 1, "_id": 0}}
    ]
    users_children = users_collection.aggregate(users_pipeline).next()["children"]
    
    
    #Getting Childrens Account Data
    childrens_collection = db["childrens"]
    childrens_pipeline = [
        {"$match": {"_id": {"$in": users_children}}},
        {"$project": {"accounts": 1, "name": 1, "_id": 0}}
    ]
    if(("children" in event.keys()) and ("all" not in event["children"])):
        childrens_pipeline[0]["$match"].update({"name": {"$in": event["children"]}})

    childrens_account = childrens_collection.aggregate(childrens_pipeline).next()["accounts"]
    
    
    #Getting Accounts Content Data
    accounts_collection = db["accounts"]
    accounts_pipeline = [
        {"$match": {"_id": {"$in": childrens_account}}},
        {"$project": {"content": 1, "_id": 0}}
    ]
    if(("platform" in event.keys()) and ("all" not in event["platform"])):
        accounts_pipeline[0]["$match"].update({"platform": {"$in": event["platform"]}})
    
    accounts_content = accounts_collection.aggregate(accounts_pipeline)
    for document in accounts_content:
        contents_id = document.get("content", [])
    
    
    #Getting Contents Comment Data
    contents_collection = db["contents"]
    contents_pipeline = [
        {"$match": {"_id": {"$in": contents_id}}},
        {"$project": {"text": 1, "comments": 1, "_id": 0}}
    ]
    if("timerange" in event.keys()):
        contents_pipeline[0]["$match"].update({"time": {"$gte": datetime.fromisoformat(event["timerange"][0]), "$lte": datetime.fromisoformat(event["timerange"][1])}})
    
    all_contents = []
    comments_id = []
    contents_comment = contents_collection.aggregate(contents_pipeline)
    for document in contents_comment:
        all_contents.append(document.get("text"))
        comments_id.append(document.get("comments"))
    
    
    #Getting Comments Data
    comments_collection = db["comments"]
    comments_pipeline = [
        {"$match": {"_id": {"$in": sum(comments_id, [])}}},
        {"$project": {"text": 1, "_id": 0}}
    ]
    
    all_comments = []
    comments = comments_collection.aggregate(comments_pipeline)
    for document in comments:
        all_comments.append(document.get("text"))
    
    return {
        'statusCode': 200,
        'body': json.dumps({"content": all_contents, "comments": all_comments})
    }
