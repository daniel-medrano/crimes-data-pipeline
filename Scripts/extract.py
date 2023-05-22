import requests
import boto3
from datetime import datetime

base_url = 'https://pjcrdatosabiertos.blob.core.windows.net/datosabiertos/PJCROD_POLICIALES_V1/'
base_file_name = 'PJCROD_POLICIALES_V1-'
file_format_ending = '.csv'
bucket_name = 'data-challenge-ulacit-2023'
prefix = 'other/' #'crimes-dataset'
s3_client = boto3.client('s3')


def download_file_from_url(url, file_name):
    response = requests.get(url)
    with open(file_name, 'wb') as file:
        file.write(response.content)

def upload_file_to_s3(file_name, bucket_name, prefix):
    s3_client.upload_file(file_name, bucket_name, prefix + file_name)

def get_name_last_file_uploaded(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response['Contents']
    for obj in objects:
        if obj['Key'].endswith('/'):
            objects.remove(obj)
    return objects[-1]['Key'] if objects != [] else None

def extract():
    last_year = datetime.today().year - 1
    name_last_file_uploaded = get_name_last_file_uploaded(bucket_name, prefix)
    if name_last_file_uploaded is not None:
        year_last_file_uploaded = int(name_last_file_uploaded.split('-')[-1].split('.')[0])

        years = list(range(year_last_file_uploaded, last_year))
        file_names = [base_file_name + str(year) + file_format_ending for year in years]
    else:
        file_names = [base_file_name + str(last_year) + file_format_ending]

    for file_name in file_names:
        url = base_url + file_name
        print('Downloading file ' + file_name)
        download_file_from_url(url, file_name)
        print('Uploading file ' + file_name + ' to s3://' + bucket_name + '/' + prefix)
        upload_file_to_s3(file_name, bucket_name, prefix)

extract()

