{{
  config(
    materialized='table'
  )
}}

select sid, name
from {{ source('staging', 'field_of_study') }}