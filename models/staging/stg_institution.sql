
{%
 set map = [
    ('v. ', 'v.'),
    (',', ''),
    ('–', '-')
 ]
%}

with institution_base as (
    select name, 
        ico as reg_number, 
        street, 
        psc as postal_code, 
        town, 
        legal_form, 
        cast(main_goal as varchar(2000)) main_goal, 
        created 
    from {{ source('staging', 'institution') }}
),
{% for old, new in map %}
institution_normalized_{{ loop.index }} as (
    select replace(name, '{{ old }}', '{{ new }}') name, 
        reg_number, 
        street, 
        postal_code, 
        town, 
        legal_form, 
        main_goal, 
        created 
    from {% if loop.first %}institution_base{% else %}institution_normalized_{{ loop.index0 }}{% endif %}
){% if not loop.last %},{% endif %}
{% endfor %}
select 
    row_number() over (order by name) as iid,    
    * 
from institution_normalized_3;