#Importing Libraries
import json, io
import boto3
import pandas as pd
from datetime import datetime, timedelta


#file_data
def file_data(s3_raw_loc, yesterday_date, current_date, collection):
    bucketName = s3_raw_loc.split("/")[2]
    bucket = s3_client.Bucket(bucketName)
    
    if(s3_raw_loc.endswith("/")):
        folder_key = "/".join(s3_raw_loc.split("/")[3:-1])
    else:
        folder_key = "/".join(s3_raw_loc.split("/")[3:])
    
    prefixes = ["/".join([folder_key, i, collection]) for i in [yesterday_date, current_date]]
    
    df_list = []
    for prefix in prefixes:
        for obj in bucket.objects.filter(Prefix = prefix):
            body = obj.get()['Body'].read()
            df = pd.read_csv(io.BytesIO(body), encoding = 'utf8')        
            df_list.append(df)
    
    total_df = pd.concat(df_list)
    total_df.reset_index(drop = True, inplace= True)
    columns = [i + f"_{collection}" for i in total_df.columns]
    total_df.columns = columns
    
    return total_df


#Setting up Boto3 Client
region_name = "ap-southeast-2"
secret_name = "dashboard"
current_date = datetime.now().strftime("%d-%m-%Y")
yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

session = boto3.session.Session(aws_access_key_id = "AKIAQBTIQ6VDCHHWNCNV", aws_secret_access_key = "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")
sm_client = session.client(service_name = "secretsmanager", region_name = region_name)
s3_client = session.resource("s3")


#Reading Data from Secrets Manager
try:
    get_secret_value_response = sm_client.get_secret_value(SecretId = secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
except Exception as e:
    print(e)

s3_raw_loc = value["raw_location"]
s3_filter_loc = value["filter_comments_location"]


#Filtering Comments
childrens_df = file_data(s3_raw_loc, yesterday_date, current_date, "childrens")
accounts_df = file_data(s3_raw_loc, yesterday_date, current_date, "accounts")
contents_df = file_data(s3_raw_loc, yesterday_date, current_date, "contents")

childrens_accounts_df = pd.merge(childrens_df, accounts_df, left_on = "accounts_childrens", right_on = "_id_accounts", how = "inner")
childrens_accounts_contents_df =  pd.merge(childrens_accounts_df, contents_df, left_on = "content_accounts", right_on = "_id_contents", how = "inner")

filter_bucket_name = s3_filter_loc.split("/")[2]
filter_key = "/".join(s3_filter_loc.split("/")[3:]) + "filter.txt"

object = s3_client.Object(filter_bucket_name, filter_key)
object.put(Body = '\n'.join([i for i in childrens_accounts_contents_df["comments_contents"].unique() if str(i) != "nan"]))
