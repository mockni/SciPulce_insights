-- =============================================================================
--  fct_publications_par_mois.sql — table de faits agrégée
--  Nombre de publications par mois et par catégorie primaire
-- =============================================================================

select
    date_trunc('month', date_published)::date   as mois,
    primary_category,
    domain,
    count(*)                                    as nb_publications,
    round(avg(abstract_length))::int            as longueur_moyenne_abstract,
    round(avg(nb_auteurs), 2)                   as nb_auteurs_moyen,
    count(*) filter (where hn_score is not null) as nb_discutes_sur_hn
from {{ ref('stg_arxiv_papers') }}
where date_published is not null
group by 1, 2, 3
