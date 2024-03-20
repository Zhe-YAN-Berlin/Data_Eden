1 API for GBQ, GCS and Cloud Dataflow are enabled for service account -> or just give Editor Role
2 auth : 
    gcloud auth login 
3 python command

python 1_aggr_bike_rides_between_stations.py \
    --project my-zhe-414813 \
    --job_name ml6-task-1 \
    --region us-central1 \
    --runner DataflowRunner \
    --staging_location gs://ml6-zhe-beam/staging
    --setup_file ./0_1_setup.py

then on dataflow UI the pipeline task/job will be there 
