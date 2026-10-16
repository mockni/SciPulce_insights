-- =============================================================================
--  stg_arxiv_papers.sql — couche staging (vue)
--  - filtre les lignes sans arxiv_id ni abstract
--  - type les colonnes (la couche raw est 100 % TEXT)
--  - extrait la catégorie primaire, le domaine et des métriques simples
-- =============================================================================

with source as (
    select * from {{ source('raw', 'arxiv_papers_raw') }}
),

cleaned as (
    select
        arxiv_id,
        nullif(trim(regexp_replace(title, '\s+', ' ', 'g')), '')            as title,
        nullif(trim(regexp_replace(abstract, '\s+', ' ', 'g')), '')         as abstract,
        nullif(trim(authors_flat), '')                                      as authors_flat,
        nullif(trim(categories), '')                                        as categories,
        coalesce(nullif(primary_category, ''), split_part(categories, ' ', 1)) as primary_category,
        -- les dates arrivent au format ISO 8601 (2023-02-07T00:00:00.000Z)
        (left(date_published, 10))::date                                    as date_published,
        (left(date_updated, 10))::date                                      as date_updated,
        nullif(doi, '')                                                     as doi,
        nullif(keywords, '')                                                as keywords,
        round(hn_score::numeric)::integer                                   as hn_score
    from source
    where arxiv_id is not null
      and abstract is not null
)

select
    arxiv_id,
    title,
    abstract,
    authors_flat,
    categories,
    primary_category,
    split_part(primary_category, '.', 1)                                     as domain,
    coalesce(date_published, date_updated)                                  as date_published,
    date_updated,
    extract(year from coalesce(date_published, date_updated))::int          as annee_publication,
    doi,
    keywords,
    hn_score,
    length(abstract)                                                         as abstract_length,
    coalesce(array_length(string_to_array(categories, ' '), 1), 0)          as nb_categories,
    coalesce(array_length(string_to_array(authors_flat, ','), 1), 0)        as nb_auteurs
from cleaned
