{#
  Par défaut, dbt préfixe les schémas personnalisés par le schéma cible
  (public_staging, public_marts). On veut exactement « staging » et « marts »,
  les schémas créés par docker/postgres/init.sql.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
