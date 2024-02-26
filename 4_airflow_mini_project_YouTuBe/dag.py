from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from googleapiclient.discovery import build
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from io import BytesIO
import pandas_gbq

args = {'owner': 'airflow'}

dag = DAG('zhe_dag', default_args=args, schedule_interval='@hourly', start_date=datetime(2023, 11, 19))

def task_1():
    # keep the key as private
    api_key = 'AIzaSyBQ9Pt_XXXXXX'

    youtube = build('youtube', 'v3', developerKey=api_key)

    channel_response = youtube.channels().list(
        part='statistics',
        id='UCo_IB5145EVNcf8hw1Kku7w'  
    ).execute()

    now = datetime.now()
    viewCount = channel_response['items'][0]['statistics']['viewCount']
    subscriberCount = channel_response['items'][0]['statistics']['subscriberCount']
    videoCount = channel_response['items'][0]['statistics']['videoCount']

    print(now,viewCount,subscriberCount,videoCount)

    bucket_name = 'lunarx_channel_data'
    filename = 'channel_daily_data.csv'
    project_name = 'My First Project'

    # Initialize a GCS client and get the bucket
    storage_client = storage.Client(project_name)
    bucket = storage_client.get_bucket(bucket_name)

    # Create a blob and write the file
    blob = bucket.blob(filename)

    # Download the current blob data as string
    csv_data = blob.download_as_text()
    csv_data += f"\n{now},{viewCount},{subscriberCount},{videoCount}"

    # Upload the updated data to the blob
    blob.upload_from_string(csv_data)

def task_2():
    from google.cloud import storage
    from google.cloud import bigquery
    import pandas as pd
    from io import BytesIO
    import pandas_gbq

    # intialize
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    bucket_name = 'lunarx_channel_data' 
    blob_name = 'channel_daily_data.csv' 

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # download csv
    blob_download = blob.download_as_bytes()

    # data to Pandas DataFrame
    data = pd.read_csv(BytesIO(blob_download), dtype={'viewCount':'int64', 'subscriberCount':'int64', 'videoCount':'int64'}, parse_dates=['current_date'])

    # data write into BQ
    table_id = "scenic-bolt-405512.lunarx.channel_overview"
    project_id = "scenic-bolt-405512"

    pandas_gbq.to_gbq(data, destination_table=table_id, project_id=project_id, if_exists='replace')

python_task_1 = PythonOperator(task_id='task_1', python_callable=task_1, dag=dag)
python_task_2 = PythonOperator(task_id='task_2', python_callable=task_2, dag=dag)

python_task_1 >> python_task_2