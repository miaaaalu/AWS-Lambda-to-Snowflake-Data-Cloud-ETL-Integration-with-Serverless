import pandas as pd
import boto3
import io
from io import StringIO

def lambda_handler(event, context):
    s3_file_key = event['Records'][0]['s3']['object']['key'];
    bucket = 'src-bucket-datapipeline';
    s3 = boto3.client('s3', aws_access_key_id='AKIAU2DYEPIAZB7BHABG',  aws_secret_access_key='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')
    obj = s3.get_object(Bucket=bucket, Key=s3_file_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()));

    service_name = 's3'
    region_name = 'ap-southeast-2'
    aws_access_key_id = 'AKIAU2DYEPIAZB7BHABG'
    aws_secret_access_key = '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'

    s3_resource = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    bucket='dst-bucket-pipeline';
    pd.options.mode.chained_assignment = None
    df1 = df[df['events'].str.contains('206', na=False)]
    
    # Convert DateTime to mintue grain
    df1['Date_Time'] = pd.to_datetime(df1['DateTime']).dt.tz_convert(None).dt.strftime('%Y-%m-%d %H:%M')

    # New columns for DimDate table 
    df1['Date'] = pd.to_datetime(df1['Date_Time']).dt.date
    df1['Year'] = pd.to_datetime(df1['Date_Time']).dt.year
    df1['Quarter'] = pd.to_datetime(df1['Date_Time']).dt.quarter
    df1['Month'] = pd.to_datetime(df1['Date_Time']).dt.month
    df1['Week'] = pd.to_datetime(df1['Date_Time']).dt.isocalendar().week
    df1['Day'] = pd.to_datetime(df1['Date_Time']).dt.day
    df1['Hour'] = pd.to_datetime(df1['Date_Time']).dt.hour
    df1['Minute'] = pd.to_datetime(df1['Date_Time']).dt.minute
    
    #split by |
    df1[['0','1','2','3','4']] = df1['VideoTitle'].str.split("|",expand=True,n=4).reindex(range(5), axis=1)

    # note: for [0] ['news', 'App Web', 'App Android', 'App iPhone', 'App iPad']
    # Create DimVideo Primary Key
    df1['Video_Title'] = df1.iloc[:, 1:].ffill(axis=1).iloc[:, -1] # Create Video_Title column
    df1['Video_ID'] = df1.groupby(['Video_Title']).ngroup() # create souragate key 
    df1['Video_ID'] = 'T' + df1['Video_ID'].astype(str)
    dfvideo= df1.drop_duplicates(subset = ["Video_Title","Video_ID"])

    # Create DimPlatform Primary Key
    df1.loc[df1['0'].str.contains('Android'), 'Platform_Type'] = 'Platform'
    df1.loc[df1['0'].str.contains('iPad'), 'Platform_Type'] = 'Platform'
    df1.loc[df1['0'].str.contains('iPhone'), 'Platform_Type'] = 'Platform'
    df1.loc[df1['0'].str.contains('Web'), 'Platform_Type'] = 'Desktop'
    df1.loc[df1['0'].str.contains('news'), 'Platform_Type'] = 'Desktop'
    df1['Platform_ID'] = df1.groupby(['Platform_Type']).ngroup() # create souragate key 
    df1['Platform_ID'] = 'P' + df1['Platform_ID'].astype(str) # add string to ID
    dfplatform= df1.drop_duplicates(subset = ["Platform_Type","Platform_ID"])

    # Create DimSite Primary Key
    df1.loc[df1['0'].str.contains('news'), 'Site'] = 'news' # Create Column 'Site' by news
    df1.loc[df1['0'].str.contains('Web'), 'Site'] = 'App Web'  # Create Column 'Site' by web
    df1.loc[df1['Site'].isnull(), 'Site'] = 'Not Applicable' #fill NaN value 
    df1['Site_ID'] = df1.groupby(['Site']).ngroup() # create souragate key 
    df1['Site_ID'] = 'S' + df1['Site_ID'].astype(str) # add string to ID
    dfsite= df1.drop_duplicates(subset = ["Site","Site_ID"])

    # Export to S3
    s3_resource.Object(bucket, 'dimdate/dimdateV2.csv').put(Body=df1[['Date_Time','Date','Year','Quarter','Month','Week','Day','Hour','Minute']].to_csv(index=False))
    s3_resource.Object(bucket, 'dimvideo/dimvideoV2.csv').put(Body=dfvideo[['Video_ID','Video_Title']].to_csv(index = False))
    s3_resource.Object(bucket, 'dimplatform/dimplatformV2.csv').put(Body=dfplatform[['Platform_ID','Platform_Type']].to_csv(index = False))
    s3_resource.Object(bucket, 'dimsite/dimsiteV2.csv').put(Body=dfsite[['Site_ID','Site']].to_csv(index = False))
    s3_resource.Object(bucket, 'facttable/facttableV2.csv').put(Body=df1[['Date_Time','Platform_ID','Site_ID','Video_ID','events']].to_csv(index = False))