{{
  config(
    materialized='table'
  )
}}

select row_number() over (order by issn) as jid, name, issn, eissn, czech_or_slovak
from (
  select distinct name, issn, eissn, czech_or_slovak
  from {{ source('staging', 'journal') }}
) t