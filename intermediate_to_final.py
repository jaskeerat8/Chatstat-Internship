#Importing Libraries
import random
import json, io
import boto3
import pandas as pd
from datetime import datetime, timedelta


#Setting up Boto3 Client
region_name = "ap-southeast-2"
secret_name = "dashboard"
current_date = datetime.now().strftime("%d-%m-%Y")
current_datetime = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")

session = boto3.session.Session(aws_access_key_id = "AKIAQBTIQ6VDCHHWNCNV", aws_secret_access_key = "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")
sm_client = session.client(service_name = "secretsmanager", region_name = region_name)
s3_client = session.resource("s3")


#Reading Data from Secrets Manager
try:
    get_secret_value_response = sm_client.get_secret_value(SecretId = secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
except Exception as e:
    print(e)

s3_inter_loc = value["inter_location"]
s3_final_loc = value["final_location"]


#Manipulating Data in final and intermediate s3 folder
bucket_name = s3_final_loc.split("/")[2]
s3_bucket = s3_client.Bucket(bucket_name)
final_folder_key = "/".join(s3_final_loc.split("/")[3:])
inter_folder_key = "/".join(s3_inter_loc.split("/")[3:]) + f"{current_date}/"
file_list = list(s3_bucket.objects.filter(Prefix = final_folder_key))


#To check if a final file is already written
final_flag = 0 
if( not ((len(file_list) == 0) or (len(file_list) == 1 and file_list[0].key == final_folder_key)) ):

    final_flag = 1
    final_df_list = []

    for obj in s3_bucket.objects.filter(Prefix = final_folder_key):
        if(obj.key.endswith(".csv")):
            body = obj.get()['Body'].read()
            df = pd.read_csv(io.BytesIO(body), encoding = 'utf8')        
            final_df_list.append(df)

    final_df = pd.concat(final_df_list)
    final_df = final_df.fillna("")
    final_df[["name_childrens", "age_childrens", "gender_childrens", "username_accounts", "platform_accounts", "platform_contents", "alert_contents", "platform_comments", "alert_comments"]] = final_df[["name_childrens", "age_childrens", "gender_childrens", "username_accounts", "platform_accounts", "platform_contents", "alert_contents", "platform_comments", "alert_comments"]].apply(lambda x: x.str.title().str.strip())
    final_df.reset_index(drop = True, inplace = True)
    print("final_flag:", str(final_flag), "file found")
else:
    print("final_flag:", str(final_flag), "file not found")


inter_df_list = []
for obj in s3_bucket.objects.filter(Prefix = inter_folder_key):
    if(obj.key.endswith(".csv")):
        body = obj.get()['Body'].read()
        df = pd.read_csv(io.BytesIO(body), encoding = 'utf8')
        inter_df_list.append(df)

inter_df = pd.concat(inter_df_list)
inter_df = inter_df.fillna("")
inter_df[["name_childrens", "age_childrens", "gender_childrens", "username_accounts", "platform_accounts", "platform_contents", "alert_contents", "platform_comments", "alert_comments"]] = inter_df[["name_childrens", "age_childrens", "gender_childrens", "username_accounts", "platform_accounts", "platform_contents", "alert_contents", "platform_comments", "alert_comments"]].apply(lambda x: x.str.title().str.strip())
inter_df.reset_index(drop = True, inplace= True)


#Pushing data to final s3
if(final_flag == 1):
    
    ############### DELETE THIS ###############
    final_df.drop(["school"], axis = 1, inplace = True)
    ############### DELETE THIS ###############
    
    total_df = pd.concat([final_df, inter_df])
    total_df.drop_duplicates(inplace = True, keep = "last")
else:
    inter_df.drop_duplicates(inplace = True)
    total_df = inter_df.copy()

############### DELETE THIS ###############
if("school" not in total_df.columns):
    total_df["school"] = [random.choice(["a", "b", "c", "d"]) for _ in range(len(total_df))]
############### DELETE THIS ###############

csv_string = total_df.to_csv(index = False, sep = ',', quotechar = '"')

#Deleting Already existing data in final folder
for obj in s3_bucket.objects.filter(Prefix = final_folder_key):
    s3_client.Object(bucket_name, obj.key).delete()

#Pushing final CSV
s3_client.Object(bucket_name, final_folder_key + f"final_{current_datetime}.csv").put(Body = csv_string)
print(final_folder_key + f"final_{current_datetime}.csv")
