# Importing Libraries
import os
import json
import boto3
import pandas as pd
from sqlalchemy import create_engine

def get_data():
    # Setting up Boto3 Client
    region_name = "ap-southeast-2"
    secret_name = "rdsMYSQL"
    session = boto3.session.Session(region_name=region_name, aws_access_key_id=os.environ.get("aws_access_key_id"),
                                    aws_secret_access_key=os.environ.get("aws_secret_access_key"))
    sm_client = session.client(service_name="secretsmanager")

    # Reading Data from Secrets Manager
    try:
        get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
        value = json.loads(get_secret_value_response["SecretString"])
    except Exception as e:
        print(e)

    # Establish Connection to MySQL
    mysql_user = value["user"]
    mysql_password = value["password"]
    mysql_host = value["endpoint"]
    mysql_db = value["database"]

    mysql_connection = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}"
    mysql_engine = create_engine(mysql_connection)

    try:
        table_name = value["parent_table"]
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=mysql_engine)
    finally:
        mysql_engine.dispose()
    return df

if __name__ == "__main__":
    print("File For Reading AWS MySQL Data")
