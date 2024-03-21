import google
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import os

# service acc 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/datatalks_jan/.google/credentials/google_credentials.json"
options = PipelineOptions()

### define functions for ETL ###
#   define extract col_1,2,3 & rental_id as single record to a tuple
def extract_columns(row):
    return (row['start_station_id'], row['end_station_id'], row['distance']), 1

#   define function to count total distance of rides
def count_total_distance(start_end_distance, counts):
    start_station_id, end_station_id, distance = start_end_distance
    return start_station_id, end_station_id, sum(counts), int(round(distance*sum(counts)/1000))

#   define function to format : untuple & newline
class FormatOutput(beam.DoFn):
    def process(self, element):
        formatted_lines = []
        for triple in element:
            formatted_line = ",".join(str(i) for i in triple)
            formatted_lines.append(formatted_line)
        return formatted_lines

# build beam pipeline
with beam.Pipeline(options=options) as pipeline:
    data =(
    pipeline
    | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(
        query="""
            SELECT start_station_id, end_station_id
            ,ST_DISTANCE(ST_GEOGPOINT(t2.longitude, t2.latitude), ST_GEOGPOINT(t3.longitude, t3.latitude)) as distance
            , rental_id
            FROM `bigquery-public-data.london_bicycles.cycle_hire` as t1
            LEFT JOIN `bigquery-public-data.london_bicycles.cycle_stations` as t2 on t2.id = t1.start_station_id
            LEFT JOIN `bigquery-public-data.london_bicycles.cycle_stations` as t3 on t3.id = t1.end_station_id
        """,
        use_standard_sql=True,
        project='my-zhe-414813',
        gcs_location='gs://ml6-zhe-beam/tmp')
    #   | 'Filter None values' >> beam.Filter(lambda row: None not in row)  #tried, didn't work
    | 'Filter non-null rows' >> beam.Filter(lambda row: all(value is not None for value in row.values()))
    #   | 'Filter non-null rows' >> beam.Filter(lambda row: row['start_station_id'] is not None and row['end_station_id'] is not None) # tried, also worked
    | 'Extract four target columns' >> beam.Map(extract_columns)
    | 'GroupBy 1st & 2nd & 3rd cols' >> beam.GroupByKey()
    | 'count rental_id' >> beam.MapTuple(count_total_distance)
    | 'Sort rental_id' >> beam.combiners.Top.Largest(100, key=lambda x: x[3])
    | 'untuple & newline' >> beam.ParDo(FormatOutput())
    )
#   final output to GCS   #
    data | 'Write to GCS as text file' >> WriteToText(
        file_path_prefix='gs://ml6-zhe-beam/output/output_task_2_1.txt',
        num_shards=1,
        shard_name_template='') 