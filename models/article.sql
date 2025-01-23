{{
  config(
    materialized='table'
  )
}}

select row_number() over (order by name) aid, 
	jid, UT_WoS, name, type, year, author_count, institution_count
from (
	select distinct (
		select jid
		from {{ ref('journal') }} j 
		where (ar.journal_name = j.name and ar.issn = j.issn and ar.czech_or_slovak = j.czech_or_slovak) or
			(ar.journal_name = j.name and '****-****' = j.issn and ar.eissn = j.eissn and ar.czech_or_slovak = j.czech_or_slovak) or
			(ar.journal_name = j.name and ar.issn = j.eissn and ar.czech_or_slovak = j.czech_or_slovak)
	) jid, ar.UT_WoS, ar.name, ar.type, ar.year, ar.author_count, ar.institution_count
	from {{ source('staging', 'article')}} ar
) t;