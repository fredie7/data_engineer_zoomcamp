#!/usr/bin/env python
# coding: utf-8

# In[7]:


# !uv add pandas


# In[10]:


import pandas as pd
from sqlalchemy import create_engine

year=2021
month=1

pg_user = 'root'
pg_password = 'root'
pg_host = 'localhost'
pg_port = 5432
pg_db = 'ny_taxi'

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz', nrows=100)



# In[11]:


# Display first rows
df.head()


# In[12]:


# Check data types
df.dtypes


# In[13]:


# Check data shape
df.shape


# In[14]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[15]:


df.dtypes


# In[17]:


# !uv add sqlalchemy psycopg2-binary


# In[18]:


engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')


# In[19]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[20]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[40]:


prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = prefix + 'yellow_tripdata_2021-01.csv.gz'


# In[41]:


df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# In[31]:


df = next(df_iter)


# In[32]:


df.head()


# In[33]:


for df_chunk in df_iter:
    print(len(df_chunk))


# In[34]:


# !uv add tqdm


# In[35]:


from tqdm.auto import tqdm


# In[42]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[37]:





# In[ ]:




