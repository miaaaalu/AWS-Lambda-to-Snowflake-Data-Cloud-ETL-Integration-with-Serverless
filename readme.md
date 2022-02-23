# Overview of the solution
The project includes a local environment to inspect the data and deploy the stack using the AWS S3, Lambda in Serverless Framework, AWS SNS, and Snowflake Snowpipe. 

The deployment includes an obect-based event that triggers an AWS Lambda function. The function ingests and stores the raw data from an external source, transforms the content, and saves the clean information. 

The raw and clean data is stored in a S3 destination bucket with a SQS event notifications,which will deliver data to snowpipe

The following diagram illustrates my architecture:

![alt text](https://github.com/miaaaalu/AWS-Lambda-to-Snowflake-Data-Cloud-ETL-Integration-with-Serverless/blob/master/ETL_Pipeline.jpg?raw=true)

The solution includes the following high-level steps:

1. Design the star schema for source file 
2. Snowflake Snowpipe Configuration
3. Build and test ETL process.
2. Inspect the function and the AWS Cloudformation template and deploy in Serverless Framwork.
5. Create an Event Notification for S3 destination bucket. 

# Preparation

### AWS
```powershell
1. Create source bucket in S3
2. Create deploy bucket for serverless package in S3 
3. Create IAM User for snowflake with S3 full access policies
```
### Python 
```powershell
3.8 preferable
```

# Step 1 — Snowflake Snowpipe Configuration
```powershell
1. Dataabase Creation
2. Table Creation (5 tables in this project)
3. File Format  Creation with AWS Crendentials
4. Pipe Creation 
For details check snowflake.sql
```

# Step 2 — Serverless Framework Deploy

### 1. Serverless Project Initialization
```powershell
% serverless create --template aws-python3 --path {project_folder}
```

### 2. Open the project in VScode
```powershell
% open -a “Visual Studio Code” ${project_folder}
```

### 3. Serverless Plugin Installation
```powershell
# Instal Serverless Prune Plugin 
% sudo sls plugin install -n serverless-prune-plugin

# Install serverless python requirements (https://github.com/UnitedIncome/serverless-python-requirements)s
% sudo sls plugin install -n serverless-python-requirements

# Install serverless dotenv plugin
% sudo npm i -D serverless-dotenv-plugin
```
### 4. Modify .python file for ETL Process
```powershell
pandas library is by default not available in AWS Lambda Python environments. For using pandas library in Lambda function, a requirements.txt needs to be attached, OR a Lambda Layer needs to attached to the Lambda function. 

# Rename python file
% mv handler.py ${project_handle}.py

# Handle your Python packaging
# option 1: attach a requirements.txt with needed library
% touch requirements.txt
% pip install -r requirements.txt
% echo “pandas” >> requirements.txt

# option 2: add the layer from Klayers to your lambda function in serverless.yml (recommend)
https://github.com/keithrozario/Klayers/tree/master/deployments/python3.8/arns

# ETL Process:
For details check etl_process.py
```
### 4. Create .env file and put environment variables if need
```env
APPLICATION=your project name
STAGE=your stage
REGION=your region
TZ_LOCAL=your timezone
```
### 5. Modify serverless.yml file
```Powershell
For details serverless.yml
```

### 6. Deploy and Cleaning up 
```Powershell
# Deploy to aws 
% sls deploy

# Delete the stack from aws 
% sls remove
```

# Step 3 — Event Notification for S3 Bucket

```
Create an Event Notification for target S3 bucket. This notification informs Snowpipe via an SQS queue when files are ready to load. Please note the ARN of the SQS queue for the stage from the notification_channel column once you execute show pipes command. Copy the ARN to a notepad.
```