select dispatching_base_num as dispatch_base,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    PUlocationID as pulocationid,
    DOlocationID as dolocationid,
    SR_Flag as SR_Flag,
    Affiliated_base_number as affiliated_base
from {{ source('staging', 'fhv_tripdata') }}
where dispatching_base_num is not null