import s3fs
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def connect_to_aws_s3():
    try:
        s3 = s3fs.S3FileSystem(anon=False, key= AWS_ACCESS_KEY_ID, secret=AWS_ACCESS_KEY)
        return s3
    except Exception as err:
        print(err)

def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket:str):
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print("Bucket created")
        else :
            print("Bucket already exists")
    except Exception as e:
        print(e)


def upload_to_aws_s3(s3: s3fs.S3FileSystem, file_path: str, bucket:str, aws_s3_file_name: str):
    try:
        s3.put(file_path, bucket+'/raw/'+ aws_s3_file_name)
        print(f'{aws_s3_file_name} has been uploaded to AWS S3')
    except FileNotFoundError:
        print(f'{aws_s3_file_name} was not found')