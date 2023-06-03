import boto3
from el import check_missing_files, download_file_from_url, upload_file_to_s3

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    base_url = 'https://pjcrdatosabiertos.blob.core.windows.net/datosabiertos/PJCROD_POLICIALES_V1/'
    base_file_name = 'PJCROD_POLICIALES_V1-'
    file_format_ending = '.csv'
    bucket_name = 'data-challenge-ulacit-2023'
    prefix = 'other/' #'crimes-dataset'
    
    file_names = check_missing_files(s3_client, base_file_name, file_format_ending, bucket_name, prefix)

    for file_name in file_names:
        download_file_from_url(base_url + file_name, '/tmp/', file_name)
        upload_file_to_s3(s3_client, '/tmp/', file_name, bucket_name, prefix)
