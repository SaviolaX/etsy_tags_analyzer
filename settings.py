import boto3

# AWS client connection
aws_credentials = {
    "key": 'AKIASHIZLRLLRTVTOLVG',
    "secret": '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'
}
s3 = boto3.client(
    's3',
    aws_access_key_id='AKIASHIZLRLLRTVTOLVG',
    aws_secret_access_key='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')
BUCKET_NAME = 'temporary-document-storage'
