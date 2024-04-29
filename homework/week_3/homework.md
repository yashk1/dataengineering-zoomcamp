## Module 3 Homework

**Important Note:**

For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found here: [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.
Stop with loading the files into a bucket.

NOTE: you will need to use the PARQUET option files when creating an External Table

**SETUP**:
Create an external table using the Green Taxi Trip Records Data for 2022.
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).


# Setup

ETL data from API to Google Cloud Storage using Mage

```
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(*args, **kwargs):


    #passing year and month as variables
    year = kwargs['year']
    month = int(kwargs['month'])

    data_link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    df = pd.read_parquet(data_link, engine = 'pyarrow')

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```


**Data Export**

```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import os

import pyarrow as pa
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# os.environment['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/prismatic-grail-420403-bcb18000f73b.json'
# bucket_name = 

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    year = kwargs['year']
    month = int(kwargs['month'])
    bucket_name = 'prismatic-grail-420403-homework-bucket'
    object_key = f'green_taxi_data/green_tripdata_{year}-{month:02d}.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )

```

## Creating Table in BigQuery

```
CREATE OR REPLACE EXTERNAL TABLE `prismatic-grail-420403.ny_taxi.external_table_green_taxi_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://prismatic-grail-420403-homework-bucket/green_taxi_data/*']
);


CREATE OR REPLACE TABLE `prismatic-grail-420403.ny_taxi.materialized_green_taxi_data`
AS 
SELECT * FROM `prismatic-grail-420403.ny_taxi.external_table_green_taxi_data`;

```

## Question 1:

Question 1: What is count of records for the 2022 Green Taxi Data??

- 65,623,481
- 840,402
- 1,936,423
- 253,647


## Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.`</br>`
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

## Question 3:

How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- 1,622

## Question 4:

What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

## Question 5:

Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)`</br>`

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? `</br>`

Choose the answer which most closely matches.`</br>`

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

## Question 6:

Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

## Question 7:

It is best practice in Big Query to always cluster your data:

- True
- False

## (Bonus: Not worth points) Question 8:

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3
