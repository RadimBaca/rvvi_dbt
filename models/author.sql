{{
  config(
    materialized='table'
  )
}}

SELECT row_number() over (ORDER BY name) rid,
  name,
  null vedidk
FROM (
  SELECT DISTINCT LTRIM(RTRIM(value)) AS name
  FROM {{ source('staging', 'article') }}
  CROSS APPLY STRING_SPLIT(authors, ';')
  WHERE LTRIM(RTRIM(value)) <> ''
) t;