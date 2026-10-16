-- ============================================================================
--  Initialisation PostgreSQL — exécuté au premier démarrage du conteneur
--  Crée la base `scipulse` utilisée par dbt (séparée de la base `airflow`)
--  ⚠️ Ce script ne s'exécute que si le volume pg-data est vide
--     (sinon : docker compose down -v puis docker compose up -d)
-- ============================================================================

CREATE USER scipulse WITH PASSWORD 'scipulse';
CREATE DATABASE scipulse OWNER scipulse;

\c scipulse

-- Schémas cibles des modèles dbt (voir dbt/macros/generate_schema_name.sql)
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION scipulse;
CREATE SCHEMA IF NOT EXISTS marts   AUTHORIZATION scipulse;

-- La table brute public.arxiv_papers_raw est créée par
-- src/transformation/load_to_postgres.py (DROP + CREATE, idempotent)
GRANT ALL ON SCHEMA public TO scipulse;
