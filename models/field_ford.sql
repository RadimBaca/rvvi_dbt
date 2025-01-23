{{
  config(
    materialized='table'
  )
}}

select fid, sid, name
from {{ source('staging', 'field_ford') }}