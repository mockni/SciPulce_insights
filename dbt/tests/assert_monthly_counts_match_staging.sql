-- Test singulier : l'agrégat mensuel doit couvrir exactement tous les articles datés
-- (aucune ligne perdue ni dupliquée par le GROUP BY).
with totals as (
    select
        (select count(*) from {{ ref('stg_arxiv_papers') }} where date_published is not null) as nb_staging,
        (select coalesce(sum(nb_publications), 0) from {{ ref('fct_publications_par_mois') }})  as nb_mart
)
select * from totals where nb_staging <> nb_mart
