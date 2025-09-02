import os
import boto3
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

def lambda_handler(event, context):
    # Getting Google credentials stored in secretsmanager
    secrets = boto3.client('secretsmanager')
    creds = json.loads(secrets.get_secret_value(
        SecretId='ibmetl-google-creds')['SecretString'])
    
    # Initializing Google Drive service
    credentials = Credentials(
        token=None,
        refresh_token=creds['refresh_token'],
        client_id=creds['client_id'],
        client_secret=creds['client_secret'],
        token_uri='https://oauth2.googleapis.com/token'
    )
    
    if credentials.expired:
        credentials.refresh(Request())
    
    service = build('drive', 'v3', credentials=credentials)
    
    # Querying files on google drive 
    query = "mimeType='application/pdf' or mimeType contains 'image/'"
    results = service.files().list(
        q=query,
        pageSize=100,
        fields="files(id,name,mimeType,modifiedTime)"
    ).execute()
    
    # Process files
    s3 = boto3.client('s3')
    file_metadata = []
    
    for file in results.get('files', []):
        # Download file content
        request = service.files().get_media(fileId=file['id'])
        content = request.execute()
        
        # Upload to S3
        s3_key = f"raw/{file['id']}_{file['name']}"
        s3.put_object(
            Bucket='ibmetl-raw-files',
            Key=s3_key,
            Body=content
        )
        
        file_metadata.append({
            'file_id': file['id'],
            'name': file['name'],
            'type': file['mimeType'],
            's3_location': f"s3://ibmetl-raw-files/{s3_key}"
        })
    
    # Save metadata
    s3.put_object(
        Bucket='ibmetl-metadata',
        Key='file_metadata.json',
        Body=json.dumps(file_metadata))
    
    return {
        'statusCode': 200,
        'body': f"Processed {len(file_metadata)} files"
    }