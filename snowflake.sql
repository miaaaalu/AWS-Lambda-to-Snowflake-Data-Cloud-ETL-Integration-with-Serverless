//create datawarehouse 
create or replace warehouse mywarehouse with
  warehouse_size='X-SMALL'
  auto_suspend = 120
  auto_resume = true
  initially_suspended=true;

//create database
CREATE OR REPLACE DATABASE DATA_PIPELINE;

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

//create a stage
CREATE OR REPLACE STAGE S3_to_Snowflake_Stage
    URL="S3://dst-bucket-pipeline"
    CREDENTIALS = (AWS_KEY_ID = 'AKIAU2DYEPIAZB7BHABG' AWS_SECRET_KEY = 'H67rq0YEdAZMLtn+xS5Dc89zgaaEScGKGUBcRhjl')
    file_format = DataPipeline_CSV_Format;

//create a pipe
CREATE OR REPLACE PIPE DimDate_Pipe
    AUTO_INGEST = TRUE 
    AS 
    COPY INTO DIMDATE 
    FROM @S3_to_Snowflake_Stage/DimDate.csv
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);
    
CREATE OR REPLACE PIPE Dimplatform_Pipe
    AUTO_INGEST = TRUE 
    AS 
    COPY INTO DIMPLATFORM 
    FROM @S3_to_Snowflake_Stage/Dimplatform.csv
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);
    
    
CREATE OR REPLACE PIPE DimSite_Pipe
    AUTO_INGEST = TRUE 
    AS 
    COPY INTO DIMSITE 
    FROM @S3_to_Snowflake_Stage/DimSite.csv
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);
    
CREATE OR REPLACE PIPE DimVideo_Pipe
    AUTO_INGEST = TRUE 
    AS 
    COPY INTO DIMVIDEO 
    FROM @S3_to_Snowflake_Stage/Dimvideo.csv
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);

CREATE OR REPLACE PIPE FactTable_Pipe
    AUTO_INGEST = TRUE 
    AS 
    COPY INTO FactTable 
    FROM @S3_to_Snowflake_Stage/FactTable.csv
    FILE_FORMAT = (FORMAT_NAME = DataPipeline_CSV_Format);

//check pipes
SHOW PIPES;

//check satging layer to see if file are successfully uploaed
list @S3_to_Snowflake_Stage;  

//save notification_channel url for S3 Event Notification
arn:aws:sqs:ap-southeast-2:486855810640:sf-snowpipe-AIDAXCWW6DZILFHBVQMMV-C8qof2NHZLas0q_FNvbOcw

//connect to snowsql 
snowsql -a rj53482.ap-southeast-2 -u MIALU;

SELECT * FROM DIMDATE;