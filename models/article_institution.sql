{{
  config(
    materialized='table'
  )
}}

SELECT DISTINCT a.aid, (
  select i.iid
  from {{ ref('institution') }} i 
  where LTRIM(RTRIM(replace(replace(split.value,'v. ','v.'), ',', ''))) = i.name
) iid
FROM {{ source('staging', 'article') }} ar
JOIN {{ ref('article') }} a ON ar.name = a.name
CROSS APPLY STRING_SPLIT(ar.VO, ';') AS split
WHERE split.value not in ('nan', 'NaN', 'N/A') and exists (
  select i.iid
  from {{ ref('institution') }} i 
  where LTRIM(RTRIM(replace(replace(split.value,'v. ','v.'), ',', ''))) = i.name
);
