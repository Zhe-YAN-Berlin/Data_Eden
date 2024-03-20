1 API for GBQ, GCS and Cloud Dataflow are enabled for service account -> or just give Editor Role
2 auth : 
    gcloud auth login 
3 python command

test ->

python 0_2_test_function.py \
    --project my-zhe-414813 \
    --job_name ml6-task-test-1 \
    --region us-west1 \
    --runner DataflowRunner \
    --staging_location gs://ml6-zhe-beam/staging
    --temp_location gs://ml6-zhe-beam/temlpate
    --setup_file ./0_1_setup.py

then on dataflow UI the pipeline task/job will be there 
