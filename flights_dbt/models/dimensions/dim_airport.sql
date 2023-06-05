{{ config(pre_hook="create table if not exists airport_log (dbt_id int,is_start_run int, is_finish int);insert into airport_log values({{var('dbt_id')}}::int,1,0)",post_hook=["update airport_log set is_finish=1 where dbt_id={{var('dbt_id')}}","insert into {{this}} select '-1','na','na','na','na',null,'na',null,null"] )}}

with airports_data as(
  select a.*
      ,'{{run_started_at.strftime("%y-%m-%d %H:%M:%S")}}' as dbt_run_time
from {{source('stg','airports_data')}} a
)

select ad.airport_code
	  ,replace((ad.airport_name -> 'en')::varchar,'"','') as airport_name_english
	  ,replace((airport_name -> 'ru')::varchar,'"','') as airport_name_russian
	  ,replace((ad.city -> 'en')::varchar,'"','') as city_english
	  ,replace((ad.city -> 'ru')::varchar,'"','') as city_russian
	  ,ad.coordinates
	  ,ad.timezone
    ,ad.dbt_run_time
	  ,ad.last_update
from airports_data ad
