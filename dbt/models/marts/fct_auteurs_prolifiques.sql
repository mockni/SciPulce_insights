-- =============================================================================
--  fct_auteurs_prolifiques.sql
--  Mart : classement des auteurs par nombre de publications
--
--  Ce modèle dépend de stg_arxiv_papers (via ref()).
--  dbt exécutera stg_arxiv_papers en premier automatiquement.
-- =============================================================================

with papers as (
    select * from {{ ref('stg_arxiv_papers') }}
),

-- Éclater les auteurs (un auteur par ligne)
-- Le champ authors_flat contient "Alice Martin, Bob Chen, ..."
authors_exploded as (
    select
        arxiv_id,
        primary_category,
        domain,
        date_published,
        trim(unnest(string_to_array(authors_flat, ','))) as author_name
    from papers
    where authors_flat is not null
      and authors_flat != ''
),

-- Agréger par auteur
author_stats as (
    select
        author_name,
        count(distinct arxiv_id)                    as nb_publications,
        count(distinct primary_category)            as nb_categories,
        min(date_published)                         as premiere_publication,
        max(date_published)                         as derniere_publication,
        mode() within group (order by domain)       as domaine_principal
    from authors_exploded
    where length(author_name) > 2     -- Filtrer les artefacts de parsing
    group by author_name
)

select
    author_name,
    nb_publications,
    nb_categories,
    premiere_publication,
    derniere_publication,
    domaine_principal,
    -- Score d'activité : publications × diversité
    nb_publications * nb_categories                 as score_activite
from author_stats
where nb_publications >= 3     -- Au moins 3 publications
order by nb_publications desc
