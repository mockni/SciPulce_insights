-- =============================================================================
--  dim_categories.sql — dimension des catégories ArXiv
--  Hiérarchie domaine / sous-domaine, libellé (seed) et volumes observés.
--  Toutes les catégories sont prises en compte (primaires ET secondaires).
-- =============================================================================

with all_categories as (
    select
        arxiv_id,
        unnest(string_to_array(categories, ' ')) as category_code,
        primary_category
    from {{ ref('stg_arxiv_papers') }}
),

stats as (
    select
        category_code,
        count(distinct arxiv_id)                                          as nb_articles,
        count(distinct arxiv_id) filter (where category_code = primary_category) as nb_articles_primaire
    from all_categories
    where category_code <> ''
    group by category_code
)

select
    s.category_code,
    split_part(s.category_code, '.', 1)                                    as domaine,
    nullif(split_part(s.category_code, '.', 2), '')                        as sous_domaine,
    coalesce(l.label, s.category_code)                                     as libelle,
    s.nb_articles,
    s.nb_articles_primaire
from stats s
left join {{ ref('arxiv_category_labels') }} l
    on l.category_code = s.category_code
