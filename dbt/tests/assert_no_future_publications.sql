-- Test singulier : aucune publication ne peut être datée dans le futur.
-- dbt considère le test en échec si la requête retourne au moins une ligne.
select arxiv_id, date_published
from {{ ref('stg_arxiv_papers') }}
where date_published > current_date
