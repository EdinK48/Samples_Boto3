import json
import os
from PIL import Image
import io
import boto3
from typing import List, Tuple



def get_aws_session(env_name: str) -> boto3.Session.resource:
    """
    Retrieve the AWS S3 client.
    
    Args:
        env_name: Environment variable for the aws profile 
    
    Returns:
        session: boto3 session
    """
    aws_profile_name = os.getenv(env_name)
    session = boto3.Session(profile_name=aws_profile_name)
    return session


def get_aws_s3_resource(session: boto3.Session) -> boto3.Session.resource: 
    """
    Retrieve the AWS S3 resource.
    
    Args:
        session: boto3 session
    
    Returns:
        s3: S3 resource for accessing S3 buckets
    """
    s3 = session.resource("s3")
    return s3

def get_s3_client(session: boto3.Session) -> boto3.Session.client:
    """
    Retrieve the AWS S3 client.
    
    Args:
        session: boto3 session
    
    Returns:
        s3_client: S3 client for accessing S3 buckets
    """
    s3_client = session.client('s3')
    return s3_client


def get_files_based_on_ending(ending: str,
                              bucket_name: str,
                              subdirectory_path: str,
                              s3: boto3.Session.resource
                              ) -> Tuple[List[str], boto3.Session.resource]:
    """
    Get all file names based on ending for each bucket & subdirectory.
    
    Args:
        ending: File ending
        bucket_name: Name of the S3 bucket
        subdirectory_path: Folder in specified bucket
        s3: The S3 client to access the S3 buckets

    Returns:
        files_names: file names from the subdirectory_path of the specified bucket
        bucket: Object used to retrieve the content of the json files
    """
    bucket = s3.Bucket(bucket_name)
    files_names = [obj.key for obj in bucket.objects.filter(Prefix=subdirectory_path) if obj.key.endswith(ending)]
    return files_names, bucket



def get_json_contents_from_aws(file_name: str, bucket) -> List[dict]:
    """
    Get the content of each json files
    
    Args:
        file_name: json file name
        bucket_name: Name of the S3 bucket

    Returns:
        json_content: List of dictionaires that contains the metadata of each file.
    """
    obj = bucket.Object(file_name)
    response = obj.get()
    file_content = response['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def read_image_from_s3(bucket_name: str,
                       object_key: str,
                       s3_client: boto3.Session.client
                       ):
    """
    Read an image file from an S3 bucket and load it as a PIL Image.

    Args:
        bucket_name: Name of the S3 bucket.
        object_key: Key (path/filename) of the file in the S3 bucket
        s3_client: S3 client for accessing S3 buckets

    Returns:
        PIL.Image.Image: The image loaded into a PIL Image object
    """
    # Download the file content to an in-memory buffer
    buffer = io.BytesIO()
    s3_client.download_fileobj(bucket_name, object_key, buffer)
    
    # Reset the buffer's position to the beginning
    buffer.seek(0)
    
    # Load the image with PIL
    image = Image.open(buffer)
    return image

def transfer_to_kili_intra_account(
    source_bucket: str,
    destination_bucket: str,
    subdirectory_path=str,
    file_to_transfer=str,
    s3=boto3.Session.resource):
    """
    Transfer image within the same account where the kili-labeling bucket resides.

    Args:
        source_bucket: Source S3 bucket where an image came from
        destination_bucket: Destination S3 bucket where an image is sent to
        subdirectory_path: ubfolder in the kili-labeling bucket which corresponds to
            a project in the kili labeling tool.
        file_to_transfer: .jpeg or .jpg to transfer to the kili-labeling bucket
        s3: The S3 resource to access the S3 buckets
    """
    copy_source = {'Bucket': source_bucket, 'Key': file_to_transfer}
    destination_file_key = subdirectory_path + file_to_transfer
    s3.meta.client.copy(
        copy_source,
        destination_bucket,
        destination_file_key,
        ExtraArgs={"ContentType": "image/jpeg",
                "MetadataDirective": "REPLACE"}
    )
    print(f"Images of project {subdirectory_path} have been transferred to kili-labeling successfully")


def upload_image_to_s3(image: Image.Image,
                       bucket_name: str,
                       object_key: str,
                       s3_client: boto3.Session.client):
    """
    Upload a PIL image to an S3 bucket under a specific subdirectory.

    Args:
        image: The PIL image to upload
        bucket_name: Name of the S3 bucket.
        object_key: Key (path/filename) of the image in the S3 bucket
        s3_client: The S3 client to access the S3 buckets
    """
    buffer = io.BytesIO()
    
    # Save the PIL image as a JPEG to the buffer
    image.save(buffer, format="JPEG")
    
    buffer.seek(0)
    
    s3_client.upload_fileobj(
        buffer,
        bucket_name,
        object_key,
        ExtraArgs={"ContentType": "image/jpeg"}
    )
    
    print(f"In {bucket_name}, the file {object_key} is successfully uploaded")
    
    