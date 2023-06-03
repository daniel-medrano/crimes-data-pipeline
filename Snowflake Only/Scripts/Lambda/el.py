import urllib3
from datetime import datetime


def download_file_from_url(url, path, file_name):
    http = urllib3.PoolManager()
    print('Downloading file ' + file_name)
    response = http.request('GET', url)

    with open(path + file_name, 'wb') as file:
        print('Writing file ' + file_name)
        print('Status code: ' + str(response.status))
        file.write(response.data)

    http.clear()

def upload_file_to_s3(s3_client, path, file_name, bucket_name, prefix):
    print('Uploading file ' + file_name + ' to s3://' + bucket_name + '/' + prefix)
    s3_client.upload_file(path + file_name, bucket_name, prefix + file_name)

def get_name_last_file_uploaded(s3_client, bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response['Contents']
    for obj in objects:
        if obj['Key'].endswith('/'):
            objects.remove(obj)
    return objects[-1]['Key'] if objects != [] else None

def check_missing_files(s3_client, base_file_name, file_format_ending, bucket_name, prefix):
    last_year = datetime.today().year - 1
    name_last_file_uploaded = get_name_last_file_uploaded(s3_client, bucket_name, prefix)
    if name_last_file_uploaded is not None:
        print('Last file uploaded: ' + name_last_file_uploaded)
        year_last_file_uploaded = int(name_last_file_uploaded.split('-')[-1].split('.')[0])
        years = list(range(year_last_file_uploaded, last_year))
        return [base_file_name + str(year) + file_format_ending for year in years]
    else:
        print('A file has not been found in the specified location of the bucket')
        print('Attempting to download last year\'s file')
        return [base_file_name + str(last_year) + file_format_ending]