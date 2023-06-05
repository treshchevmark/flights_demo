{{config(post_hook="insert into {{this}} select -1, 'na','na', null,'na','na','na',null")}}

with aircrafts_data as(
  select a.*
        ,'{{run_started_at.strftime("%y-%m-%d %H:%M:%S")}}' as dbt_run_time
  from {{source('stg','aircrafts_data')}} a
)
, seats as (
  select *
  from {{source('stg','seats')}}
)
select ad.aircraft_code
      ,replace((ad.model -> 'en')::varchar,'"','') as model_english
      ,replace((ad.model -> 'ru')::varchar,'"','') as model_russian
  	  ,ad."range"
  	  ,case when ad."range" > 5600 then 'high' else 'low' end as range_desc
  	  ,s.seat_no
  	  ,s.fare_conditions
      ,ad.dbt_run_time
from aircrafts_data ad
left join seats s
on ad.aircraft_code = s.aircraft_code
