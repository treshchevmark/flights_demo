{{ config(materialized='incremental',unique_key='book_ref') }}

with bookings as(
  select *
  from {{source('stg','bookings')}} b
)
,tickets as(
  select t.*
        ,'{{run_started_at.strftime("%y-%m-%d %H:%M:%S")}}' as dbt_run_time
  from {{source('stg','tickets')}} t
)

select b.book_ref
  	  ,b.book_date
  	  ,b.total_amount
  	  ,t.ticket_no
  	  ,t.passenger_id
  	  ,t.passenger_name
  	  ,replace((t.contact_data -> 'phone')::varchar,'"','') as phone
  	  ,replace((t.contact_data -> 'email')::varchar,'"','') as email
  	  ,t.last_update as last_update_tickets
      ,t.dbt_run_time
from bookings b
left join tickets t
on b.book_ref = t.book_ref
where 1=1
{% if is_incremental() %}
and b.book_date > (select max(book_date) from {{ this }})
{% endif %}
