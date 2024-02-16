#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd


# In[4]:


pd.__version__


# In[5]:


df = pd.read_csv('green_tripdata_2019-01.csv', nrows=100)


# In[6]:


df


# In[7]:


print(pd.io.sql.get_schema(df, name = 'green_texi_data'))


# In[8]:


pd.to_datetime(df.lpep_pickup_datetime)


# In[9]:


df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


# In[10]:


from sqlalchemy import create_engine


# In[11]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[12]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[13]:


from sqlalchemy import create_engine


# In[14]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()


# In[15]:


print(pd.io.sql.get_schema(df, name = 'green_texi_data', con=engine))


# In[16]:


df_iter = pd.read_csv('green_tripdata_2019-01.csv', iterator=True, chunksize=100000)


# In[17]:


df_iter = pd.read_csv('green_tripdata_2019-01.csv', iterator=True, chunksize=100000)
df=next(df_iter)
df


# In[18]:


df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime) #text转timestamp
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


# In[19]:


df.head()


# In[28]:


df.head(n=0)


# In[20]:


df.head(n=0).to_sql(name = 'green_texi_data',con=engine,if_exists='replace')


# In[21]:


get_ipython().run_line_magic('time', "df.to_sql(name='green_texi_data', con=engine, if_exists='append')")


# In[ ]:


from time import time
while True:
    t_start=time()
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime) #text转timestamp
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.to_sql(name='green_texi_data', con=engine, if_exists='append')
    t_end = time()
    print('insert another chunk... took %.3f sec' % (t_end - t_start))


# In[ ]:


from time import time
while True:
    t_start=time()
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime) #text转timestamp
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.to_sql(name='green_texi_data', con=engine, if_exists='append')
    t_end = time()
    print('insert another chunk... took %.3f sec' % (t_end - t_start))


# In[38]:


print('insert another chunk... took %.3f sec' % (t_end - t_start))


# In[39]:


df = pd.read_csv('green_tripdata_2019-01.csv')


# In[40]:


len(df)

