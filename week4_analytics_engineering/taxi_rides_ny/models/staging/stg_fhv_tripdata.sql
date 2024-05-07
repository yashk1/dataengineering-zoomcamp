{{
    config(
        materialized='view'
    )
}}

with fhv_tripdata as(
    select * 
from {{ source('staging','external_fhv_tripdata') }}
)


select
    to_hex(md5(cast(coalesce(cast(pulocationid as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime as string), '_dbt_utils_surrogate_key_null_') as string))) as tripid,
    safe_cast(dispatching_base_num as INT64) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    safe_cast(pulocationid as INT64) as pickup_locationid,
    safe_cast(dolocationid as INT64) as dropoff_locationid,
    sr_flag,
    affiliated_base_number

from fhv_tripdata
--where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run': false}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}