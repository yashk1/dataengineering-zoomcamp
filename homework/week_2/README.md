## Module 2 Homework

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format, please include these directly in the README file of your repository.

> In case you don't get one option exactly, select the closest one

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

To get a `wget`-able link, use this prefix (note that the link itself gives 404):

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/`

### Assignment

The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to a database (and Google Cloud!).

- Create a new pipeline, call it `green_taxi_etl`
- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months `10`, `11`, `12`).
  - You can use the same datatypes and date parsing methods shown in the course.
  - `BONUS`: load the final three months using a for loop and `pd.concat`
- Add a transformer block and perform the following:
  - Remove rows where the passenger count is equal to 0 _and_ the trip distance is equal to zero.
  - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
  - Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
  - Add three assertions:
    - `vendor_id` is one of the existing values in the column (currently)
    - `passenger_count` is greater than 0
    - `trip_distance` is greater than 0
- Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.
- Write your data as Parquet files to a bucket in GCP, partioned by `lpep_pickup_date`. Use the `pyarrow` library!
- Schedule your pipeline to run daily at 5AM UTC.

```
#get the green taxi data for 2020 , 10 11 12 and store it in GCP Cloud storage
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    taxi_dtypes = {
        'VendorID': 'Int64',
        'store_and_fwd_flag': 'str',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'ehail_fee': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'float64',
        'trip_type': 'float64',
        'congestion_surcharge': 'float64'
    }
    parse_dates_green_taxi = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    # parse_dates_yellow_taxi = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    years = [2020]
    months = [10,11,12]
    dfs = []

    for year in years:
        for month in months:
            data_link = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month}.csv.gz"
            df = pd.read_csv(data_link, dtype = taxi_dtypes, parse_dates = parse_dates_green_taxi)
            dfs.append(df)
    mereged_df = pd.concat(dfs, ignore_index = True)
    return mereged_df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```

### Data Transformation

```
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.

@transformer
def transform(data, *args, **kwargs):
    print(f"Preprocessing: rows with zero passengers: {(data['passenger_count'] == 0).sum()}")
    data = data[data['passenger_count']>0]

    print(f"Preprocessing: rows with zero trip distance: {(data['trip_distance'] == 0).sum()}")
    data = data[data['trip_distance']> 0]

    #create new column 
    print("Creating new column lpep_pickup_date")
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #changing column names from camel case to snake case
    print("Preprocessing: changing column names from camel case to snake case")

    columns = {
        'VendorID': 'vendor_id',
        'RatecodeID': 'rate_code_id', 
        'PULocationID': 'pu_location_id', 
        'DOLocationID': 'do_location_id',
    }

    data = data.rename(columns = columns)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, 'vendor_id not in columns'

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output['passenger_count']==0).sum() == 0, 'Rows still available with passenger count 0'

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output['trip_distance']==0).sum() == 0, 'Rows still available with trip_distance = 0'
```

### Data export to GCS

#### 1. Export whole file

```
# Write your data as Parquet files to a bucket in GCP, partioned by lpep_pickup_date. Use the pyarrow library!


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
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'prismatic-grail-420403-homework-bucket'
    object_key = 'green_taxi_2020_10_11_12.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )

```

#### 2. Export partioned file

```
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/prismatic-grail-420403-bcb18000f73b.json'
bucket_name = 'prismatic-grail-420403-homework-bucket'
project_id = 'prismatic-grail-420403'
table_name = 'nyc_taxi_data_green'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table, 
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
```

### Questions

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns
* 544,898 rows x 18 columns
* 544,898 rows x 20 columns
* 133,744 rows x 20 columns

### Answer

```
266,855 rows x 20 columns
```

## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 _and_ the trip distance is greater than zero, how many rows are left?

* 544,897 rows
* 266,855 rows
* 139,370 rows
* 266,856 rows

### Answer

```
139,370
```

## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* `data = data['lpep_pickup_datetime'].date`
* `data('lpep_pickup_date') = data['lpep_pickup_datetime'].date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()`

### Answer

```
data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
```

## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1, 2, or 3
* 1 or 2
* 1, 2, 3, 4
* 1

### Answer

```
1 or 2
```

```
SELECT * from information_schema.tables where table_schema = 'mage';

select vendor_id from mage.green_taxi
group by vendor_id;
```

## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 3
* 6
* 2
* 4

### Answer

```
4
```

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

* 96
* 56
* 67
* 108

### Answer

```
96
```

## Submitting the solutions

* Form for submitting: [https://courses.datatalks.club/de-zoomcamp-2024/homework/hw2](https://courses.datatalks.club/de-zoomcamp-2024/homework/hw2)
