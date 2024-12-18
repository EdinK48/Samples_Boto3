from project_utils import get_projects_from_txt
from aws_utils import (get_aws_session, 
                       get_aws_s3_resource,
                       get_files_based_on_ending,
                       get_json_contents_from_aws,
                       get_s3_client,
                       read_image_from_s3,
                       transfer_to_labeltool_intra_account,
                       upload_image_to_s3)


DESTINATION_BUCKET = "<your destination bucket>"


def main():
    projects = get_projects_from_txt()
    # Get destination resources
    destination_session = get_aws_session("AWS_INT_ACCOUNT")
    s3_destination = get_aws_s3_resource(destination_session)
    s3_client_destination = get_s3_client(destination_session)
    
    # Get source resources
    source_session = get_aws_session("AWS_PROD_ACCOUNT")
    s3_client_source = get_s3_client(source_session)
    
    processed_images = []
    
    for project in projects:
        if "/" not in project:
            project += "/"
        json_files, bucket_obj_destination = get_files_based_on_ending(ending=".json",
                                                           bucket_name=DESTINATION_BUCKET, 
                                                           subdirectory_path=project, 
                                                           s3=s3_destination)
     
        for file in json_files:
            json_content = get_json_contents_from_aws(file_name=file, 
                                                      bucket=bucket_obj_destination)
            for image_content in json_content:
                image_content_split = image_content["img_url"].split("/")
                image_name = image_content_split[-1]
                image_hash = image_name.split(".")[0]
                if image_hash not in processed_images:
                    source_bucket_name = image_content_split[2].split(".")[0]
                    if "int" in source_bucket_name:
                        print(project, image_name, source_bucket_name)
                        transfer_to_labeltool_intra_account(source_bucket=source_bucket_name,
                                                    destination_bucket=DESTINATION_BUCKET,
                                                    subdirectory_path=project,
                                                    file_to_transfer=image_name,
                                                    s3=s3_destination)
                    else: 
                        pil_image = read_image_from_s3(bucket_name=source_bucket_name,
                                                    object_key=image_name,
                                                    s3_client=s3_client_source)
                        object_key = project + image_name
                        upload_image_to_s3(image= pil_image,
                                        bucket_name=DESTINATION_BUCKET,
                                        object_key=object_key,
                                        s3_client=s3_client_destination)
                    processed_images.append(image_hash)
                else: continue
    
    

if __name__ == "__main__":
    main()
