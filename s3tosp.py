import json
import boto3
import io
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
import datetime

def lambda_handler(event, context):
    # x= datetime.datetime.now()
    s3 = boto3.client('s3')
    Key =event["Records"][0]["s3"]["object"]["key"]
    file = s3.get_object(Bucket="sharepoint-project", Key=Key)
    file_name= Key.split("/")[-1]
    # list_file = s3.list_objects_v2(Bucket="sharepoint-project",Prefix= f"emp/reject/{x.year}/{x.month}/{x.day}")   
    # print(list_file)
    # list_file_new = list_file["Contents"]
    # print(event)
    
    
    def upload_to_sp(file, file_name):
        
       
        url = "https://zh7j6.sharepoint.com"
        share_site = "https://zh7j6.sharepoint.com"
        user = "shlok03@zh7j6.onmicrosoft.com"
        password = 'P!G)F9ZNnHw-m9"'
        auth = Office365(url, username=user, password=password).GetCookies()
        site = Site(share_site, version=Version.v365, authcookie=auth)
        folder = site.Folder('Shared Documents/data/reject')

        folder.upload_file(io.BytesIO(file["Body"].read()),file_name )
        
        
        
    upload_to_sp(file, file_name)


    
