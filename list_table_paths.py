import boto3


input_path = 'gverma-api-monitor'


folders = []
table_paths = []


def is_partition_folder(folder_name):
    if folder_name.find('=') > 0:
        return True
    else:
        return False


# create boto3 s3 client
client = boto3.client('s3')

# response = client.list_objects(Bucket = 'gverma-api-monitor', Prefix = 'covid-patients/', Delimiter = '/')
# print(response)


def list_objects(bucket_name, prefix):
    return client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')


def dig_in_sub_folder(input_path):
    print(f"input_path = {input_path}")

    bucket_name, prefix = input_path.split(
        '/', 1) if input_path.find('/') > 0 else (input_path, '')
    print(f"bucket_name = {bucket_name}, prefix = {prefix}")

    # list objects at input location (starting location)
    response = list_objects(bucket_name, prefix)

    # check for base case if starting folder has got any sub folders
    sub_folders = response['CommonPrefixes'] if 'CommonPrefixes' in response else table_paths.append(
        input_path)
    print(sub_folders)

    # if starting folder has got sub folders iterate over each one
    if sub_folders:
        for sub_folder in sub_folders:
            if is_partition_folder(sub_folder['Prefix']):
                table_paths.append(input_path)
                break
            else:
                dig_in_sub_folder(bucket_name + '/' + sub_folder['Prefix'])


dig_in_sub_folder(input_path)
print(table_paths)


# 1. input can be bucket or subfolder path
# 2. if input path doesn't contain any nested folder then it is table path trigger job and exit
# 	 else call recursive funtion and collect all table paths
