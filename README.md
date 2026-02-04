# Agrégation et traitement de données énergétiques des bâtiments en France

Ce projet permet **d’agréger et de traiter différentes sources de données liées aux bâtiments en France**, notamment :

- Données de **consommation d’eau, consommation de gaz, consommation d’électricité**
- Un fichier de **conditions climatiques**
- Un fichier contenant les **tarifs de l’eau, de l’électricité et du gaz**

## Objectif du projet

L’objectif est de produire :

- Un **ensemble de données traitées** (formats `.csv` ou `.parquet`)
- Des **graphiques explicatifs** (figures) permettant d’analyser les consommations et leurs liens avec le climat et les tarifs

Les résultats sont écrits dans le dossier `output/`.
Par souci de place, les fichiers de résultats trop volumineux (1.7 Go)  n'ont pas été poussés sur le gestionnaire de version.

## Prérequis techniques

- Docker
- Docker Compose
- Python 3.x (pour l’exécution hors Docker si nécessaire)

## Dépendances Python

Les dépendances Python sont listées dans le fichier :

```text
requirements.txt
```

Il est recommandé de créer un environnement virtuel, par exemple avec venv.
Une fois ce dernier activé : 
````bash
pip install -r requirements.txt
````

## Structure

````
├── docker-compose.yml
├── requirements.txt
├── data/
├── notebooks/
│   └── 01_exploration_spark.py (etc...)
└── output/
│   └── figures

````

## Lancement du runner spark sur Docker

```bash
docker compose up -d
```