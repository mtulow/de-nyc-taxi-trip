select
    VendorID as vendorid,
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID as ratecodeid,
    PULocationID as pickup_locationid,
    DOLocationID as dropoff_locationid,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
from
    staging.green_taxi_trips
limit 100;