#Importing Libraries
import io
import json
import boto3
import pymysql
import pandas as pd
from sqlalchemy import create_engine


#Setting up Boto3 Client
region_name = "ap-southeast-2"
secret_name = "rdsMYSQL"

session = boto3.session.Session(region_name = region_name)
sm_client = session.client(service_name = "secretsmanager")
s3_client = session.resource("s3")


#Reading Data from Secrets Manager
try:
    get_secret_value_response = sm_client.get_secret_value(SecretId = secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
except Exception as e:
    print(e)


#Reading the database connection credentials
host = value["endpoint"]
user = value["user"]
password = value["password"]
database = value["database"]
table = value["parent_table"]
final_location = value["final_location"]


#Read Final Data
bucket_name = final_location.split("/")[2]
prefix = "/".join(final_location.split("/")[3:])
bucket = s3_client.Bucket(bucket_name)

df_list = []
for obj in bucket.objects.filter(Prefix = prefix):
    s3_object = s3_client.Object(bucket_name, obj.key)
    file_content = s3_object.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(file_content))
    df_list.append(df)

df = pd.concat(df_list, axis = 0, ignore_index = True)
df.fillna("", inplace = True)


#Connect to the database
try:
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    print("Successfully Connected to Database")
except Exception as e:
    print("Connection failed: ", e)


#Inserting DataFrame into DataBase
df.to_sql(con = engine, name = table, if_exists = "replace", index = False)
print("DataFrame Inserted")
