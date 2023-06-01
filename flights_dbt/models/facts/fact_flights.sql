{{ config(materialized='incremental',unique_key='flight_id') }}

with flights as(
  select f.*
      	,'{{run_started_at.strftime("%y-%m-%d %H:%M:%S")}}' as dbt_run_time
  from {{source('stg','flights')}} f
)

select f.flight_id
		  ,f.flight_no
		  ,f.scheduled_departure
		  ,f.scheduled_arrival
		  ,f.departure_airport
		  ,f.arrival_airport
		  ,f.status
		  ,f.aircraft_code
		  ,f.actual_departure
		  ,f.actual_arrival
		  ,round((extract(epoch from f.actual_arrival - f.actual_departure)/(60*60))::numeric,2) as flight_duration
		  ,round((extract(epoch from f.scheduled_arrival - f.scheduled_departure)/(60*60))::numeric,2) as flight_hours_expected
		  ,case when actual_departure is null then null else (case when (extract(epoch from f.scheduled_arrival - f.scheduled_departure)/(60*60)-extract(epoch from f.actual_arrival - f.actual_departure)/(60*60)) = 0 then 'As Expected'
		        												 when (extract(epoch from f.scheduled_arrival - f.scheduled_departure)/(60*60)-extract(epoch from f.actual_arrival - f.actual_departure)/(60*60)) > 0 then 'Shorter'
		        												 else 'Longer' end)
		   end as duration_expected_than_actual
			,f.last_update
			,f.dbt_run_time
from flights f
where 1=1
{% if is_incremental() %}
and f.last_update > (select max(last_update) from {{ this }})
{% endif %}
