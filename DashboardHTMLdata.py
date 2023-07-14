#Importing Libraries
import io
import json
import boto3
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta


#pandas data
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
current_date = datetime.now().strftime("%d-%m-%Y")
yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

region_name = "ap-southeast-2"
secret_name = "rdsMYSQL"

session = boto3.session.Session(aws_access_key_id = "AKIAQBTIQ6VDCHHWNCNV", aws_secret_access_key = "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")
sm_client = session.client(service_name = "secretsmanager", region_name = region_name)
s3_client = session.resource("s3")


#Reading Data from Secrets Manager
try:
    get_secret_value_response = sm_client.get_secret_value(SecretId = secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
except Exception as e:
    print(e)


#Reading data from RAW
s3_raw_loc = value["raw_location"]
childrens_df = file_data(s3_raw_loc, yesterday_date, current_date, "childrens")
accounts_df = file_data(s3_raw_loc, yesterday_date, current_date, "accounts")

childrens_accounts_df = pd.merge(childrens_df, accounts_df, left_on = "accounts_childrens", right_on = "_id_accounts", how = "inner")
childrens_accounts_df = childrens_accounts_df[["user_childrens", "name_childrens", "platform_accounts", "username_accounts"]]
childrens_accounts_df.drop_duplicates(keep = "first", inplace = True, ignore_index = True)
childrens_accounts_df.rename(columns = {"user_childrens":"chatstat_id", "name_childrens":"chatstat_name", "platform_accounts":"platform", "username_accounts":"platform_username"}, inplace = True)

final_df = pd.pivot_table(childrens_accounts_df, values = "platform_username", index = ["chatstat_id", "chatstat_name"], columns = ["platform"], aggfunc = "first").reset_index()
final_df.fillna("", inplace = True)
final_df.rename(columns = {platform : platform + "_account" for platform in childrens_accounts_df["platform"].unique()}, inplace = True)

for column in ["instagram_account", "tiktok_account", "twitter_account"]:
    if column not in final_df.columns:
        final_df[column] = ""


#Reading the database connection credentials
host = value["endpoint"]
user = value["user"]
password = value["password"]
database = value["database"]
table = value["html_table"]


#Connect to the database
try:
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    print("Successfully Connected to Database")
except Exception as e:
    print("Connection failed: ", e)


#Inserting DataFrame into DataBase
final_df.to_sql(con = engine, name = table, if_exists = "replace", index = False)
print("DataFrame Inserted")
