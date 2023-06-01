{{ config(materialized='incremental',unique_key="ticket_no||'-'||flight_id") }}


with ticket_flights as(
  select t.*
      	,'{{run_started_at.strftime("%y-%m-%d %H:%M:%S")}}' as dbt_run_time
  from {{source('stg','ticket_flights')}} t
)
, boarding_passes as (
  select *
  from {{source('stg','boarding_passes')}}
)

select t.ticket_no
  	  ,t.flight_id
  	  ,t.fare_conditions
  	  ,t.amount
  	  ,b.boarding_no
  	  ,b.seat_no
  	  ,t.last_update  as last_update_ticket_flight
  	  ,b.last_update  as last_update_boarding_pass
      ,t.dbt_run_time
from ticket_flights t
left join boarding_passes b
on b.ticket_no = t.ticket_no
   and b.flight_id = t.flight_id
where 1=1
{% if is_incremental() %}
and t.last_update > (select max(last_update_ticket_flight) from {{ this }})
{% endif %}
