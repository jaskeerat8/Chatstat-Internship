#Importing Libraries
import json
import boto3

#Setting up Boto3 Client
region_name = "ap-southeast-2"
secret_name = "dashboard"
session = boto3.session.Session(aws_access_key_id = "AKIAQBTIQ6VDCHHWNCNV", aws_secret_access_key = "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")
sm_client = session.client(service_name = "secretsmanager", region_name = region_name)
s3 = session.client("s3")

#Reading Data from Secrets Manager
try:
    get_secret_value_response = sm_client.get_secret_value(SecretId = secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
except Exception as e:
    print(e)

raw_folder = value["raw_location"].split("/")[-2]
intermediate_folder = value["inter_location"].split("/")[-2]
final_folder = value["final_location"].split("/")[-2]
filter_folder = value["filter_comments_location"].split("/")[-2]

# set the S3 bucket name
s3_location = value["s3_location"]
bucket_name = s3_location.split("/")[2]
s3_path = "/".join(s3_location.split("/")[3:])

# set the list of folders to check/create
folders = [raw_folder, intermediate_folder, final_folder, filter_folder]
folders_path = [s3_path + folder + "/" for folder in folders]

# check if each folder exists, and create it if it doesn't
for folder in folders_path:
    response = s3.list_objects_v2(
        Bucket = bucket_name,
        Prefix = folder)

    if "Contents" not in response:
        print(folder, "does not exist in S3")
        s3.put_object(Bucket=bucket_name, Key=(folder))
