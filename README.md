# Plateforme Big Data de Traitement et d’Analyse des News

Projet complet de pipeline Big Data permettant la collecte, le streaming, le traitement, le stockage, l’orchestration, le monitoring et la visualisation de données provenant de sites d’actualités.

Le projet utilise plusieurs technologies modernes telles que Kafka, Spark, Airflow, MinIO, PostgreSQL, Metabase, Prometheus et Grafana.

---

# Table des Matières

- Architecture
- Technologies Utilisées
- Structure du Projet
- Data Layers
- Services Inclus
- Variables d’Environnement
- Workflow d’Exécution
- Installation
- Interfaces
- Monitoring
- Pipeline Streaming
- DAGs Airflow
- Configuration Kafka
- Configuration PostgreSQL
- Commandes Docker
- Troubleshooting
- Fonctionnalités
- Réseau Docker
- Stockage des Données
- Future Improvements

---

# Architecture du Pipeline

```text
  +-------------+   +-------------+     
  | CNN Stream  |   | AlJazeera   |
  |  (Realtime) |   | DAG (1h)    |
  +------+------+   +------+------+ 
         |                 |
         +--------+--------+
                  |
                  v
           +-------------+
           |   Kafka     |
           +------+------+ 
                  |
                  v
       +---------------------+   
       | Bronze/Silver Layer |   
       |      (Realtime)     |     
       |        MinIO        |
       +----------+----------+ 
                  |
                  v
           +-------------+
           | Gold Layer  |
           |  DAG (1h)   |
           | PostgreSQL  |
           +------+------+ 
                  |       
                  v       
           +-------------+
           |  Metabase   | 
           +-------------+ 
```

---

# Technologies Utilisées

- Apache Kafka
- Apache Spark
- Apache Airflow
- PostgreSQL
- MinIO
- Metabase
- Prometheus
- Grafana
- Docker & Docker Compose
- Python 3.11
- Streamlit

---

# Structure du Projet

```bash
bigdata-stack/
│
├── aljazeera.py
├── cnn.py
├── docker-compose.yml
├── Dockerfile.airflow
├── prometheus.yml
├── requirements.txt
├── logs_dashboard.py
│
├── dags/
│   ├── aljazeera_dag.py
│   └── gold_layer_dag.py
│
├── init/
│   ├── init-metabase.sql
│   ├── init_metabase.py
│   │
│   ├── dashboards/
│   │   ├── dashboard_config.yml
│   │   └── main_dashboard.json
│   │
│   └── datasources/
│       └── datasource.yml
│
└── spark_jobs/
    ├── gold_processor.py
    ├── spark_processor.py
    └── verify_silver.py
```

---

# Data Layers

## Bronze Layer

Contient les données brutes récupérées directement depuis les sites d’actualités.

Stockage :
- MinIO
- Bucket : `bronze`

---

## Silver Layer

Contient les données nettoyées et transformées après traitement Spark.

Stockage :
- MinIO
- Bucket : `silver`

---

## Gold Layer

Contient les données analytiques finales.

Utilisation :
- statistiques
- dashboards
- analyses métier
- reporting BI

Stockage :
- PostgreSQL Data Warehouse

---

# Services Inclus

| Service | Description |
|---|---|
| PostgreSQL | Base de données principale |
| Kafka | Streaming temps réel |
| Zookeeper | Coordination Kafka |
| Spark Master | Gestion du cluster Spark |
| Spark Worker | Exécution des traitements Spark |
| Airflow | Orchestration ETL |
| MinIO | Stockage objet |
| Metabase | Dashboard analytique |
| Prometheus | Collecte des métriques |
| Grafana | Visualisation et monitoring |
| Streamlit Logs Dashboard | Analyse temps réel des logs |

---

# Workflow d’Exécution

Le pipeline fonctionne selon deux modes.

## Streaming Temps Réel

Les services suivants fonctionnent en continu en mode streaming :

- `cnn-streamer`
- `spark-streaming-job`

### Fonctionnement

1. `cnn-streamer` scrape les articles CNN en continu.
2. Les données sont envoyées vers Kafka.
3. `spark-streaming-job` consomme les données Kafka.
4. Spark traite les flux en temps réel.
5. Les données sont stockées dans le Bronze Layer.
6. Les données nettoyées sont envoyées dans le Silver Layer.

---

## Traitement Planifié

Les DAGs Airflow suivants sont exécutés automatiquement chaque heure :

- `aljazeera_dag.py`
- `gold_layer_dag.py`

### aljazeera_dag.py

Responsable de :
- scraper les articles Al Jazeera
- ingérer les données dans le pipeline
- envoyer les données vers Kafka

### gold_layer_dag.py

Responsable de :
- récupérer les données Silver
- générer les données Gold analytiques
- charger PostgreSQL
- mettre à jour les dashboards

---

# Installation

## Prérequis

Avant de lancer le projet, assurez-vous d’avoir :

- Docker
- Docker Compose
- Python 3.11

Vérification de la version Python :

```bash
python --version
```

Résultat attendu :

```bash
Python 3.11.x
```

---

## Cloner le Projet

```bash
git clone https://github.com/user/bigdata-stack.git
cd bigdata-stack
```

---

## Build des Conteneurs

```bash
docker compose build
```

---

## Démarrage des Services

```bash
docker compose up -d
```

---

## Vérification des Conteneurs

```bash
docker ps
```

---

# Interfaces

## Airflow

URL :

```text
http://localhost:8088/
```

Identifiants :

```text
Username : admin
Password : admin123
```

---

## Metabase

URL :

```text
http://localhost:3000/
```

Identifiants :

```text
Email : admin@admin.com
Password : AdminPassword123!
```

---

## MinIO Console

URL :

```text
http://localhost:9001/
```

Identifiants :

```text
Username : admin
Password : admin123
```

Buckets créés automatiquement :

- bronze
- silver

---

## Grafana

URL :

```text
http://localhost:3001/
```

Identifiants :

```text
Username : admin
Password : admin
```

---

## Prometheus

URL :

```text
http://localhost:9090/
```

---

## Spark Master UI

URL :

```text
http://localhost:8080/
```

---

## Spark Worker UI

URL :

```text
http://localhost:8081/
```

---

# Monitoring

## Grafana

Grafana permet :

- monitoring Spark
- monitoring Airflow
- activité streaming
- suivi système
- visualisation des métriques

---

## Prometheus

Prometheus collecte les métriques depuis :

- Spark
- Airflow
- StatsD Exporter
- système Docker

---

## Dashboard Logs Streamlit

Application web développée avec Streamlit pour surveiller et analyser les logs des conteneurs Docker en temps réel.

### Lancement

```bash
streamlit run logs_dashboard.py
```

### Catégories des Logs

| Onglet | Description |
|---|---|
| ERROR | Exceptions et erreurs critiques |
| WARN | Warnings et timeouts |
| INFO | Informations système |
| PRINTS | Résultats Spark et Airflow |

L’application permet de surveiller l’état de toute l’infrastructure Big Data.

---

# Pipeline Streaming

## CNN Streamer

Conteneur :

```text
cnn-streamer
```

Responsabilités :

- scraper CNN
- envoyer les données vers Kafka
- streaming temps réel

---

## Spark Streaming Job

Conteneur :

```text
spark-streaming-job
```

Responsabilités :

- consommer Kafka
- traitement streaming
- transformation des données
- stockage Bronze/Silver

---

# DAGs Airflow

## aljazeera_dag.py

Exécution :

```text
Chaque heure
```

Responsabilités :

- scraping Al Jazeera
- ingestion des données
- envoi pipeline

---

## gold_layer_dag.py

Exécution :

```text
Chaque heure
```

Responsabilités :

- génération Gold Layer
- chargement PostgreSQL
- alimentation dashboards

---

# Configuration PostgreSQL

Identifiants par défaut :

```text
User : admin
Password : admin123
```

Bases utilisées :

- airflow
- metabase
- warehouse

---

# Configuration Kafka

| Port | Usage |
|---|---|
| 9092 | Communication interne Docker |
| 29092 | Accès localhost |

---

# Commandes Docker Utiles

## Arrêter les Services

```bash
docker compose down
```

---

## Redémarrer les Services

```bash
docker compose restart
```

---

## Voir les Logs

```bash
docker compose logs -f
```

---

## Rebuild Complet

```bash
docker compose up --build
```

---

# Troubleshooting

## Kafka indisponible

Erreur :

```text
Connection refused kafka:9092
```

### Solution

```bash
docker compose restart kafka
```

Puis vérifier les logs :

```bash
docker compose logs -f kafka
```

---

## Spark Streaming Error

Erreur :

```text
Spark connection timeout
```

### Solution

```bash
docker compose restart spark-master spark-worker
```

---

## Airflow DAG ne démarre pas

### Solution

```bash
docker compose logs -f airflow
```

Vérifier :

- PostgreSQL
- Kafka
- présence des DAGs

---

## Python Version Error

Le projet nécessite :

```text
Python 3.11
```

---

# Fonctionnalités du Projet

- Scraping automatique
- Streaming temps réel avec Kafka
- Traitement Big Data Spark
- ETL avec Airflow
- Data Lake MinIO
- PostgreSQL Warehouse
- Monitoring Prometheus/Grafana
- Dashboards analytiques
- Logs temps réel avec Streamlit

---

# Réseau Docker

Tous les services communiquent via :

```text
bigdata-net
```

---

# Stockage des Données

Les données persistantes sont stockées dans :

```text
./data/
```

---

# Future Improvements

- NLP Sentiment Analysis
- Kubernetes Deployment
- Real-Time Alerts
- Machine Learning Predictions
- Multi-source News Aggregation

---

# Auteur
AADOUD ANASS & GOURAGUINE ZAKARIA

Projet Big Data — Plateforme de Traitement et d’Analyse des News

Technologies :
Docker, Kafka, Spark, Airflow, PostgreSQL, MinIO, Metabase, Prometheus et Grafana.
