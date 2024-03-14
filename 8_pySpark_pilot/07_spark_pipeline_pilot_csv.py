#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

credentials_location = '/home/datatalks_jan/.google/credentials/google_credentials.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test_again') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

df_green = spark.read.parquet('gs://nyc-tlc-backup/pq/green/*/*') #测试是否可以读


# In[2]:


df_green.show()


# In[10]:


from pyspark.sql import types
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

schema = StructType ([StructField('hvfhs_license_num',StringType(),True),\
                     StructField('dispatching_base_num',StringType(),True),\
                     StructField('pickup_datetime',TimestampType(),True),\
                     StructField('dropoff_datetime',TimestampType(),True),\
                     StructField('PULocationID',IntegerType(),True),\
                     StructField('DOLocationID',IntegerType(),True),\
                    StructField('SR_Flag',StringType(),True)])


# In[12]:


raw_data = spark.read.option("header",True).csv("/home/datatalks_jan/Data_Eden/8_pySpark_pilot/head_*.csv", schema=schema)


# In[13]:


raw_data.printSchema()
raw_data.count()


# In[14]:


data_without_bad = raw_data.filter(raw_data.hvfhs_license_num != "BAD") 
data_without_bad.count()


# In[15]:


from pyspark.sql.functions import col, to_date

data_older_than_summer_2021 = data_without_bad.filter(col("pickup_datetime") != "2021-01-01 00:21:08") 
data_older_than_summer_2021.count()


# In[16]:


from pyspark.sql import functions as F

data_clean=data_older_than_summer_2021.withColumn('pickup_datetime',F.to_date(data_older_than_summer_2021.pickup_datetime)) \
.withColumn('dropoff_datetime',F.to_date(data_older_than_summer_2021.dropoff_datetime)) \
.withColumn('operator', F.lit('ZHE'))


# In[17]:


data_clean.show()


# In[18]:


data_clean.createOrReplaceTempView('etl_1')


# In[19]:


final_df = spark.sql("""
SELECT
    *
FROM
    etl_1
WHERE 1=1
AND hvfhs_license_num = 'HV0003'
""")


# In[20]:


final_df.show()


# In[21]:


final_df.write.parquet("gs://nyc-tlc-backup/test_output", mode="overwrite")


# In[240]:


#locally only can run one session / context therefore we need to stop when to create new one 
#spark.stop()
#sc.stop()
#spark.sparkContext.stop()


# In[ ]:




