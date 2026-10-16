"""
SciPulse — Client MinIO
Utilitaires de connexion et lecture des objets dans MinIO (S3-compatible).

Ce module fournit :
  - get_minio_client()  : connexion au serveur MinIO
  - list_json_objects() : liste les fichiers JSON d'un bucket
  - read_json_object()  : lit et parse un fichier JSON depuis MinIO
  - iter_json_objects()  : itérateur sur tous les JSON d'un bucket (streaming)

Configuration par variables d'environnement (avec valeurs par défaut
alignées sur le docker-compose SciPulse) :
  MINIO_ENDPOINT  = http://localhost:9000  (ou localhost:9000)
  MINIO_ACCESS_KEY = minioadmin
  MINIO_SECRET_KEY = minioadmin
  MINIO_SECURE     = false

Usage :
    from src.ingestion.minio_client import get_minio_client, iter_json_objects

    client = get_minio_client()
    for filename, records in iter_json_objects(client, "arxiv-raw"):
        print(f"{filename} → {len(records)} enregistrements")
"""

import json
import logging
import os

from minio import Minio

logger = logging.getLogger(__name__)

# ── Configuration (défauts = docker-compose SciPulse) ─────────────────────

from src.config import MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from src.config import MINIO_ENDPOINT as _RAW_ENDPOINT

# Le SDK `minio` attend « host:port » sans schéma, alors que boto3 (et le .env)
# utilisent « http://host:port » : on accepte les deux formes.
MINIO_SECURE   = os.getenv("MINIO_SECURE", "false").lower() == "true" or _RAW_ENDPOINT.startswith("https://")
MINIO_ENDPOINT = _RAW_ENDPOINT.replace("https://", "").replace("http://", "").rstrip("/")


def get_minio_client() -> Minio:
    """
    Crée et retourne un client MinIO configuré.

    Retourne
    --------
    Minio
        Client prêt à l'emploi.

    Raises
    ------
    Exception
        Si la connexion échoue.
    """
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    # Vérification rapide : le serveur répond-il ?
    client.list_buckets()
    return client


def list_json_objects(client: Minio, bucket: str, prefix: str = "") -> list[str]:
    """
    Liste les fichiers .json dans un bucket MinIO.

    Paramètres
    ----------
    client : Minio
        Client MinIO connecté.
    bucket : str
        Nom du bucket (ex: "arxiv-raw").
    prefix : str
        Préfixe optionnel pour filtrer (ex: "2024/").

    Retourne
    --------
    list[str]
        Liste des noms d'objets (clés S3) se terminant par .json.
    """
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    return [
        obj.object_name
        for obj in objects
        if obj.object_name.endswith(".json") and not obj.is_dir
    ]


def read_json_object(client: Minio, bucket: str, object_name: str) -> list | dict:
    """
    Lit un objet JSON depuis MinIO et retourne le contenu parsé.

    Gère deux formats courants :
      - Un tableau JSON  : [{"id": ...}, {"id": ...}, ...]
      - Un objet JSON    : {"records": [...]} ou {"data": [...]}
      - Du JSON-Lines     : un objet JSON par ligne

    Paramètres
    ----------
    client : Minio
        Client MinIO connecté.
    bucket : str
        Nom du bucket.
    object_name : str
        Clé de l'objet dans le bucket.

    Retourne
    --------
    list | dict
        Contenu parsé. Si JSON-Lines, retourne une liste de dicts.
    """
    response = None
    try:
        response = client.get_object(bucket, object_name)
        raw_bytes = response.read()
        text = raw_bytes.decode("utf-8")

        # Tenter le parsing JSON standard
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Fallback : JSON-Lines (un objet par ligne)
        records = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning(f"Ligne non parsable dans {object_name}: {line[:80]}...")
        return records

    finally:
        if response:
            response.close()
            response.release_conn()


def iter_json_objects(client: Minio, bucket: str, prefix: str = ""):
    """
    Itérateur qui parcourt tous les fichiers JSON d'un bucket MinIO.

    Yields
    ------
    tuple[str, list]
        (nom_objet, liste_de_records) pour chaque fichier JSON.
        Si le fichier contient un dict avec une clé 'records' ou 'data',
        la liste est extraite automatiquement.
    """
    object_names = list_json_objects(client, bucket, prefix=prefix)
    logger.info(f"📂 {len(object_names)} fichier(s) JSON trouvé(s) dans s3://{bucket}/{prefix}")

    for obj_name in sorted(object_names):
        try:
            data = read_json_object(client, bucket, obj_name)

            # Normaliser : si c'est un dict, extraire la liste de records
            if isinstance(data, dict):
                # Essayer les clés courantes
                for key in ("records", "data", "items", "papers", "results"):
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break
                else:
                    # Dict unique → le mettre dans une liste
                    data = [data]

            if not isinstance(data, list):
                logger.warning(f"⚠️  {obj_name} : format inattendu ({type(data).__name__}), ignoré")
                continue

            yield obj_name, data

        except Exception as e:
            logger.error(f"❌ Erreur lors de la lecture de {obj_name} : {e}")
            continue
