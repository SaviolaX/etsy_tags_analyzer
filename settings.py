import os
import boto3

from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

SECRET_KEY = os.environ.get('SECRET_KEY')

# AWS client connection
aws_credentials = {
    "key": os.environ.get('ACCESS_KEY'),
    "secret": os.environ.get('SECRET_ACCESS_KEY')
}
s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('ACCESS_KEY'),
    aws_secret_access_key=os.environ.get('SECRET_ACCESS_KEY'))
BUCKET_NAME = os.environ.get('BUCKET_NAME')
