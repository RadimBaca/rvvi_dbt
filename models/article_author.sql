{{
  config(
    materialized='table'
  )
}}

SELECT DISTINCT a.aid, au.rid
FROM {{ source('staging', 'article') }} ar
JOIN {{ ref('article') }} a ON ar.name = a.name
CROSS APPLY STRING_SPLIT(ar.authors, ';') AS split
JOIN {{ ref('author') }} au ON LTRIM(RTRIM(split.value)) = au.name;