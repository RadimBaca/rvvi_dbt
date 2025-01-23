select aid as yid, fid, year, article_count, zone as ranking, (
	select jid 
	from {{ ref("journal") }} j
	where rj.name = j.name and j.issn = rj.issn and j.eissn = rj.eissn and j.czech_or_slovak = rj.czech_or_slovak
) jid
from {{ source('staging', 'journal') }} rj;