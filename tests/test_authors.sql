select 1 a
from (select 1 a) t
where not exists (
    select 1 
    from {{ ref('author') }} 
    where name like 'Baca, Radim'
)