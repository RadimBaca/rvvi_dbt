
select iid, coalesce(sm.new, si.name) name, reg_number, street, postal_code, town, legal_form, main_goal, created
from {{ ref('stg_institution') }} si
left join {{ ref('institution_map') }} sm on si.name = sm.origin 
