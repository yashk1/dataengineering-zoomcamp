{{
    config(
        materialized='table'
    )
}}


with fhv_tripdata as(
    select *,
        'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as(
    select * 
    from {{ ref("dim_zones") }}
)

select 
    fhv.tripid
    fhv.dispatching_base_num
    fhv.pickup_datetime
    fhv.dropoff_datetime
    fhv.pickup_locationid
    fhv.dropoff_locationid
    fhv.sr_flag
    fhv.affiliated_base_number
from external_fhv_tripdata fhv 
    inner join dim_zones as pickup_zone on fhv.locationid = pickup_zone.pulocationid
    inner join dim_zones as dropoff_zone on fhv.locationid = dropoff_zone.dolocationid

