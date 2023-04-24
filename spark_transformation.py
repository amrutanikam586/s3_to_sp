import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
import re
import boto3
import io
from io import StringIO

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
## @params: [JOB_NAME]

import json
import boto3


client = boto3.client('dynamodb')

try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME','date'])
    job.init(args['JOB_NAME'],args) 
    i=args['date']
    x=datetime.datetime.strptime(i, "%Y-%m-%d")
except:
    x=datetime.datetime.now()
print(x)
    
BUCKET_NAME = 'sharepoint-project'
# file_schema = dict({"emp.csv": ['EMPLOYEE_ID', 'FIRST-NAME', 'LAST-NAME', 'EMAIL', 'PHONE_NUMBER', 'HIRE@DATE', 'JOB_ID',
#                   'SALARY', 'COMMISSION_PCT', 'MANAGER_ID', 'DEPARTMENT_ID', 'LAST@Date']
#                   })
s3_client = boto3.client('s3')
def get_bucket_details():
    
    
                  
def size_validation(obje):
    size = obje['ContentLength']

    if size == 0:
        raise TypeError("file is empty")
    else:
        return obje
        
def create_df_s3(file_name):
    KEY = f"emp/raw/{x.year}/{x.month}/{x.day}/{file_name}"
    df_spark = spark.read.load(f"s3://{BUCKET_NAME}/{KEY}", 
                          format="csv", 
                          sep=",", 
                          inferSchema="true",
                          header="true")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=KEY)                    
    obje=size_validation(obj)
    
    return df_spark
    
def header_validation(file_name):
    df_spark = create_df_s3(file_name)
    if lst[file_name] != list(df_spark.columns):
        raise TypeError("header not valid")
    else:
        return df_spark
        
def header_transformation(file_name):
    df_spark = header_validation(file_name)
    df = df_spark.toDF(*[re.sub('[!@#$%^&*()-]', '_', c) for c in df_spark.columns])
    newdf = df.toDF(*[c.lower().strip() for c in df.columns])
    return newdf
    
def validate_email(file_name):
    newdf = header_transformation(file_name)
    df_accept_mails = newdf.filter(newdf["email"].rlike(r'[^@]+@[^@]+\.[^@]'))  # accept emails
    df2 = newdf.alias("newdf")  # copy
    df_rej_mails = df2.subtract(df_accept_mails)  #reject emails
    return newdf,df_rej_mails
    
def valid_ph_no(file_name):
    newdf, df_rej_mails = validate_email(file_name)
    df_accepted_phoneno = newdf.filter(newdf["phone_number"].rlike(r'[1-9]{1}[0-9]{2}.[0-9]{3}.[0-9]{4}'))
    df3=newdf.alias("newdf")
    df_rej_phoneno = df3.subtract(df_accepted_phoneno)
    return newdf,df_rej_mails,df_rej_phoneno
    
def upload_to_s3(file_list):
    for file_name in file_list:
        fold_name = file_name.split(".")[0]
        newdf, df_rej_mails, df_rej_phoneno = valid_ph_no(file_name)

        FileName_success = f"{fold_name}/success/{x.year}/{x.month}/{x.day}/{file_name}"
        FileName_invalid_email = f"{fold_name}/reject/{x.year}/{x.month}/{x.day}/email_{file_name}"
        FileName_invalid_ph_no = f"{fold_name}/reject/{x.year}/{x.month}/{x.day}/ph_no_{file_name}"

        csv_buffer = StringIO()
        newdf.toPandas().to_csv(csv_buffer, index=False)
        response = s3_client.put_object(Body=csv_buffer.getvalue(),
                                        Bucket=BUCKET_NAME,
                                        Key=FileName_success)

        csv_buffer = StringIO()
        df_rej_mails.toPandas().to_csv(csv_buffer, index=False)
        response = s3_client.put_object(Body=csv_buffer.getvalue(),
                                        Bucket=BUCKET_NAME,
                                        Key=FileName_invalid_email)
       

        csv_buffer = StringIO()
        df_rej_phoneno.toPandas().to_csv(csv_buffer, index=False)
        response = s3_client.put_object(Body=csv_buffer.getvalue(),
                                        Bucket=BUCKET_NAME,
                                        Key=FileName_invalid_ph_no)
        

upload_to_s3(["emp.csv"])
        

