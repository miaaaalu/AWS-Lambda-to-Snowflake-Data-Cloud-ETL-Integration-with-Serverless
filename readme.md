# Preparation

### AWS
* Create source bucket 
* Create destination bucket 
* Create Lambda Function 
* Create IAM User for snowflake with S3 full access policies

### Python 
pandas library is by default not available in AWS Lambda Python environments. If you try to import pandas in aws lambda function, you will get below error. For using pandas library in Lambda function a Lambda Layer needs to attached to the Lambda function. 
```powershell
% mkdir {folder for import libraries}
% CD {folder for import libraries}
% pip3 install pandas==1.0.3 -t .
% pip3 install boto3==1.13.11 -t .
% pip3 install s3fs==0.4.2 -t .

# or add the layer from Klayers to lambda 
https://github.com/keithrozario/Klayers/tree/master/deployments/python3.8/arns

```

# Challenge 

### Fighting with snowpipe 
snowpipe doesn't support load speicifc files to specific tables. I tried to add patterns or file names speicifc the table name. however, this is not available in snowpipe. After researching from the official document,I have two options. (1) Create different stages to my pipes, (2) Create different folders in my S3 Dstination Bucket. I don't want to create to much stages to mess up the back end, also two much stages may need to create different buckts or notifications. so I create different folders in my S3, then specify the folder path of the stage in snowpipe. Then finally sovle this issue.


