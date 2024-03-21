import google
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import os

# service acc 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/datatalks_jan/.google/credentials/google_credentials.json"
options = PipelineOptions()

### define functions for ETL ###
#   define extract col_1,2 & rental_id as single record to a tuple
def extract_columns(row):
    return (row['start_station_id'], row['end_station_id']), 1

#   define function to count rental_id
def count_rental_ids(start_n_end_station_id, counts):
    start_station_id, end_station_id = start_n_end_station_id
    return start_station_id, end_station_id, sum(counts)

#   define function to format : untuple & newline
class FormatOutput(beam.DoFn):
    def process(self, element):
        formatted_element = ", ".join(str(i) for i in element)
        return [formatted_element + '\n']

# build beam pipeline
with beam.Pipeline(options=options) as pipeline:
    data =(
    pipeline
    | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(
        query="""
            SELECT start_station_id, end_station_id, rental_id
            FROM `bigquery-public-data.london_bicycles.cycle_hire`
        """,
        use_standard_sql=True,
        project='my-zhe-414813',
        gcs_location='gs://ml6-zhe-beam/tmp')
    | 'Extract three target columns' >> beam.Map(extract_columns)
    | 'GroupBy 1st & 2nd cols' >> beam.GroupByKey()
    | 'count rental_id' >> beam.MapTuple(count_rental_ids)
    | 'Sort rental_id' >> beam.combiners.Top.Largest(100, key=lambda x: x[2])
    | 'untuple & newline' >> beam.ParDo(FormatOutput())
    )
#   final output to GCS   #
    data | 'Write to GCS as text file' >> WriteToText(
        file_path_prefix='gs://ml6-zhe-beam/output/output_task_1.txt',
        num_shards=1,
        shard_name_template='') 