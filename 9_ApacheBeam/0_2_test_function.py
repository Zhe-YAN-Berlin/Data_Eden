import google
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import os

# service acc 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/datatalks_jan/.google/credentials/google_credentials.json"
options = PipelineOptions()

# deal with every row
def process_row(row):
    return row

# build beam
with beam.Pipeline(options=options) as p:
    # read from BQ
    rows = p | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(
        query="""
            SELECT *
            FROM `bigquery-public-data.london_bicycles.cycle_hire`
            LIMIT 1
        """,
        use_standard_sql=True,
        project='my-zhe-414813',
        gcs_location='gs://ml6-zhe-beam/tmp'
    )

    # process_row
    processed_rows = rows | 'Process data' >> beam.Map(process_row)
    
    # output
    processed_rows | 'Write to GCS' >> WriteToText(
        file_path_prefix='gs://ml6-zhe-beam/output/output.txt',
        num_shards=1,
        shard_name_template=''
    )