import argparse
import google
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import os

# service acc 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/datatalks_jan/.google/credentials/google_credentials.json"

# pipeline options 
parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
options = PipelineOptions(
    beam_args,
    runner = 'DataflowRunner',
    project = 'my-zhe-414813',
    job_name = 'ml6-task-test-00',
    temp_location = 'gs://ml6-zhe-beam/temlpate',
    region = 'us-west1'
)

# deal with every row
def process_row(row):
    return row

# Custom DoFn to print a message
class PrintMessage(beam.DoFn):
    def process(self, element):
        print("After reading from BigQuery...")
        yield element

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
    
    # Output
    processed_rows | 'Print debug message' >> beam.ParDo(PrintMessage()) \
                   | 'Write to GCS' >> WriteToText(
                            file_path_prefix='gs://ml6-zhe-beam/output/output_0.txt',
                            num_shards=1,
                            shard_name_template=''
                        )
