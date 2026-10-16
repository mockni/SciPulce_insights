# =============================================================================
#  Image Airflow SciPulse
#  = image officielle + un virtualenv contenant les dépendances du pipeline
#  (remplace le « pip install » à la volée de l'étape 3 du TP 7 : les
#   dépendances survivent aux redémarrages des conteneurs)
#
#  Build : docker compose build   (fait automatiquement par docker compose up)
# =============================================================================
FROM apache/airflow:2.9.3-python3.11

USER root
COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN python -m venv /opt/scipulse-venv \
 && /opt/scipulse-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/scipulse-venv/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt \
 && chmod -R a+rX /opt/scipulse-venv

USER airflow

# Interpréteur et binaire dbt utilisés par les DAGs
ENV SCIPULSE_PYTHON=/opt/scipulse-venv/bin/python \
    SCIPULSE_DBT=/opt/scipulse-venv/bin/dbt
