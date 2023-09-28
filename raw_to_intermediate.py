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
            body = obj.get()["Body"].read()
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

session = boto3.session.Session(region_name = region_name)
sm_client = session.client(service_name = "secretsmanager")
s3_client = session.resource("s3")


#Reading Data from Secrets Manager
try:
    get_secret_value_response = sm_client.get_secret_value(SecretId = secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
except Exception as e:
    print(e)

s3_raw_loc = value["raw_location"]
s3_inter_loc = value["inter_location"]


#Reading data from RAW
users_df = file_data(s3_raw_loc, yesterday_date, current_date, "users")
childrens_df = file_data(s3_raw_loc, yesterday_date, current_date, "childrens")
accounts_df = file_data(s3_raw_loc, yesterday_date, current_date, "accounts")
contents_df = file_data(s3_raw_loc, yesterday_date, current_date, "contents")
comments_df = file_data(s3_raw_loc, yesterday_date, current_date, "comments")


#Merging Data
users_childrens_df = pd.merge(users_df, childrens_df, left_on = "children_users", right_on = "_id_childrens", how = "inner")
users_childrens_accounts_df = pd.merge(users_childrens_df, accounts_df, left_on = "accounts_childrens", right_on = "_id_accounts", how = "inner")
users_childrens_accounts_contents_df =  pd.merge(users_childrens_accounts_df, contents_df, left_on = "content_accounts", right_on = "_id_contents", how = "inner")
users_childrens_accounts_contents_comments_df = pd.merge(users_childrens_accounts_contents_df, comments_df, left_on = "comments_contents", right_on = "_id_comments", how = "left")

users_superAdmin_df = users_df[(users_df["role_users"].str.lower() == "superadmin") & ~(users_df["email_users"].isin(users_childrens_df["email_users"]))]
users_childrens_accounts_contents_comments_df = pd.concat([users_childrens_accounts_contents_comments_df, users_superAdmin_df])

users_childrens_accounts_contents_comments_df.drop_duplicates(inplace = True)
new_columns = {old_col: old_col.lstrip("_") for old_col in users_childrens_accounts_contents_comments_df.columns}
users_childrens_accounts_contents_comments_df = users_childrens_accounts_contents_comments_df.rename(columns = new_columns)
users_childrens_accounts_contents_comments_df.fillna(value = "", inplace = True)


#Saving to s3 location
s3_client = session.client("s3")
bucket = s3_inter_loc.split("/")[2]
intermediate_data_key = "/".join(s3_inter_loc.split("/")[3:]) + f"{current_date}/intermediate_{datetime.now()}.csv"

csv_string = users_childrens_accounts_contents_comments_df.to_csv(index = False)
s3_client.put_object(Bucket = bucket, Key = intermediate_data_key, Body = csv_string)
print(f"Saving file to:", "Bucket:", bucket, "Key:", intermediate_data_key)
