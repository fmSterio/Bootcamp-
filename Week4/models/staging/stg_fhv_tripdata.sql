{{ config(materialized="view") }}

with trip_data as (
    select *
    from {{ source('staging','fhv_data') }}
    where dispatching_base_num is not null
)

select 
    {{ dbt_utils.surrogate_key(['dispatching_base_num','pickup_datetime','Affiliated_base_number']) }} as tripid,
    dispatching_base_num,				
    cast(pickup_datetime as timestamp) as pickup_datetime,			
    cast(dropOff_datetime as timestamp) as dropoff_datetime,			
    cast(PUlocationID as integer) as pulocationid,				
    cast(DOlocationID as integer) as  dolocationid,				
    cast(SR_Flag as integer) as sr_flag,			
    cast(Affiliated_base_number as string) as affiliated_base_number		
from trip_data
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
