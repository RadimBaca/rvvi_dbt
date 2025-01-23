{{
  config(
    materialized='table'
  )
}}

select yid, fid, year, article_count, ranking, jid
from {{ ref('stg_year_field_journal') }} rj
where yid not in (
	select MAX(yid)
	from {{ ref('stg_year_field_journal') }} j
	join (
		select j.year, j.jid, j.fid
		from {{ ref('stg_year_field_journal') }} j
		group by j.year, j.jid, j.fid
		having COUNT(*) > 1
	) t on j.year = t.year and j.jid = t.jid and j.fid = t.fid
	group by j.year, j.jid, j.fid
);