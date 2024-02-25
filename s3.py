# Importing Libraries
import os
import json
import boto3
import pandas as pd

def get_data():
    # Setting up Boto3 Client
    region_name = "ap-southeast-2"
    secret_name = "dashboard"
    session = boto3.session.Session(region_name=region_name, aws_access_key_id=os.environ.get("aws_access_key_id"),
                                    aws_secret_access_key=os.environ.get("aws_secret_access_key"))
    sm_client = session.client(service_name="secretsmanager")
    s3_client = session.client(service_name="s3")

    # Reading Data from Secrets Manager
    try:
        get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
        value = json.loads(get_secret_value_response["SecretString"])
    except Exception as e:
        print(e)

    final_location = value["final_location"]
    bucket_name = final_location.split("/")[2]
    folder_prefix = "/".join(final_location.split("/")[3:])

    file_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    file = file_list["Contents"][0]
    file_key = file["Key"]

    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj["Body"])
    return df

if __name__ == "__main__":
    print("File For Reading Final AWS Data")
