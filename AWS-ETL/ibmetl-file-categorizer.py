import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')
metadata = json.loads(s3.get_object(
    Bucket='ibmetl-metadata',
    Key='file_metadata.json')['Body'].read().decode('utf-8'))

for file in metadata:
    src_bucket = 'ibmetl-raw-files'
    src_key = file['s3_location'].split('/')[-1]

    if 'pdf' in file['type']:
        dest_bucket = 'ibmetl-pdf-files'
    elif 'image' in file['type']:
        dest_bucket = 'ibmetl-image-files'
    else:
        dest_bucket = 'ibmetl-other-files'

    s3.copy_object(
        CopySource={'Bucket': src_bucket, 'Key': src_key},
        Bucket=dest_bucket,
        Key=file['name']
    )

    s3.delete_object(Bucket=src_bucket, Key=src_key)

job.commit()