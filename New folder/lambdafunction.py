import boto3
import os
import urllib.parse

s3 = boto3.client('s3')


IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']
PDF_EXTENSIONS = ['.pdf']

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    

    file_extension = os.path.splitext(key)[1].lower()
    
    try:

        if file_extension in PDF_EXTENSIONS:
            destination_bucket = 'processed-pdf-files-bucket-123'
        elif file_extension in IMAGE_EXTENSIONS:
            destination_bucket = 'processed-image-files-bucket-123'
        else:
            print(f'File {key} has unsupported extension {file_extension}')
            return {
                'statusCode': 200,
                'body': 'File not categorized - unsupported extension'
            }

        copy_source = {'Bucket': bucket, 'Key': key}
        s3.copy_object(
            Bucket=destination_bucket,
            Key=key,
            CopySource=copy_source
        )
        

        s3.delete_object(Bucket=bucket, Key=key)
        
        print(f'File {key} moved to {destination_bucket}')
        
        return {
            'statusCode': 200,
            'body': f'File successfully categorized to {destination_bucket}'
        }
        
    except Exception as e:
        print(e)
        print(f'Error processing file {key} from bucket {bucket}')
        raise e