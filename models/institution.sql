{{
    config({
        "materialized": 'table',
        "post-hook": [
            "{{ create_clustered_index(columns = ['iid'], unique=True) }}",
        ]
    })

}}


SELECT row_number() over (order by split.value) + (select max(iid) + 1 from {{ ref('institution_mapped')}} ) iid,
    split.value name,
    null reg_number,
    null street,
    null postal_code,
    null town,
    null legal_form,
    null main_goal,
    null created
FROM {{ source('staging', 'article') }} ar
JOIN {{ ref('article') }} a ON ar.name = a.name
CROSS APPLY STRING_SPLIT(ar.VO, ';') AS split
where (
  select COUNT(*)
  from {{ ref('institution_mapped') }} i 
  where LTRIM(RTRIM(replace(replace(split.value,'v. ','v.'), ',', ''))) = i.name
) = 0 and split.value not in ('nan', 'NaN', 'N/A')
group by split.value
    UNION ALL
select *
from {{ ref('institution_mapped') }};