//create datawarehouse 
create or replace warehouse mywarehouse with
  warehouse_size='X-SMALL'
  auto_suspend = 120
  auto_resume = true
  initially_suspended=true;

//create database
CREATE OR REPLACE DATABASE ETL_PIPELINE;

//create DimDate table
CREATE OR REPLACE TABLE DimDate (
    Date_Time TIMESTAMP,
    Date Date NOT NULL,
    Year INTEGER NOT NULL,
    Quarter INTEGER NOT NULL,
    Month INTEGER NOT NULL,
    Week INTEGER NOT NULL,
    Day INTEGER NOT NULL,
    Hour INTEGER NOT NULL,
    Minute INTEGER NOT NULL,
    PRIMARY KEY (Date_Time)
);

//create DimPlatform table
CREATE OR REPLACE TABLE DimPlatform (
    PLATFORM_ID VARCHAR(10),
    PLATFORM_TYPE VARCHAR(20) NOT NULL,
    PRIMARY KEY (PLATFORM_ID)
);

//create DimSite table
CREATE OR REPLACE TABLE DimSite (
    Site_ID VARCHAR(10),
    Site VARCHAR(50) NOT NULL,
    PRIMARY KEY (Site_ID)
);

//create DimVideo table
CREATE OR REPLACE TABLE DimVideo (
    Video_ID VARCHAR(20),
    Video_Title TEXT NOT NULL,
    PRIMARY KEY (Video_ID)
);

//create Fact table
CREATE OR REPLACE TABLE FactTable (
    Date_Time TIMESTAMP REFERENCES DimDate(Date_Time),
    Platform_ID VARCHAR(10) REFERENCES DimPlatform(Platform_ID),
    Site_ID VARCHAR(10) REFERENCES DimSite(Site_ID),
    Video_ID VARCHAR(20) REFERENCES DimVideo(Video_ID),
    events STRING NOT NULL
);

//Create a file format 
CREATE OR REPLACE FILE FORMAT DataPipeline_CSV_Format
    TYPE = 'CSV'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI'
    skip_header = 1
    field_delimiter = ','
    record_delimiter = '\\n'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

//create a external stage
CREATE OR REPLACE STAGE S3_to_Snowflake_Stage
    URL="S3://dst-bucket-snowpipeline"
    CREDENTIALS = (AWS_KEY_ID = '**************' AWS_SECRET_KEY = '**************')
    file_format = DataPipeline_CSV_Format;

//create pipes
CREATE OR REPLACE PIPE DimDate_Pipe
    AUTO_INGEST = TRUE 
    AS COPY INTO DIMDATE 
    FROM @S3_to_Snowflake_Stage/dimdate/
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);
    
CREATE OR REPLACE PIPE Dimplatform_Pipe
    AUTO_INGEST = TRUE 
    AS COPY INTO DIMPLATFORM 
    FROM @S3_to_Snowflake_Stage/dimplatform/
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);
    
CREATE OR REPLACE PIPE DimSite_Pipe
    AUTO_INGEST = TRUE 
    AS COPY INTO DIMSITE 
    FROM @S3_to_Snowflake_Stage/dimsite/
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);
    
CREATE OR REPLACE PIPE DimVideo_Pipe
    AUTO_INGEST = TRUE 
    AS COPY INTO DIMVIDEO 
    FROM @S3_to_Snowflake_Stage/dimvideo/
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);

CREATE OR REPLACE PIPE FactTable_Pipe
    AUTO_INGEST = TRUE 
    AS COPY INTO FactTable 
    FROM @S3_to_Snowflake_Stage/facttable/
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);

// PIPES
SHOW PIPES; -- check pipes to get notification_channel url
SELECT SYSTEM$PIPE_STATUS('<PIPE NAME>'); -- Check Pipe Status if need
SELECT * FROM table(information_schema.copy_history(table_name=>'<TABLE NAME>', start_time=> dateadd(hours, -1, current_timestamp()))); -- Show PIPE COPY history in specific table 
ALTER PIPE <PIPE NAME> REFRESH; -- REFRESH PIPE 

// EXTERNAL STAGE
LIST @S3_to_Snowflake_Stage; // Check files in external stage 
REMOVE '@S3_to_Snowflake_Stage/dimdate/date.csv'; //remove single file from external stage 
REMOVE @S3_to_Snowflake_Stage pattern='.*.csv';//remove all files from external stage 

// save notification_channel url for S3 Event Notification
arn:aws:sqs:ap-southeast-2:123456789012:sf-snowpipe-AIDAXCWW6DZILFHBVQMMV-C8qof2NHZLas0q_FNvbOcw