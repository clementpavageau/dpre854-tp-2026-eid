# TP Détection d'intrusion - DPRE854

## Structure du projet

```text
/
├── dags
│   ├── intrusion.py
│   ├── intrusion_2.py
│   ├── intrusion.log
│   ├── result_intrusion.csv
│   ├── ip_inventory.csv
│   ├── ip_country.csv
│   └── final_intrusion.csv
│
├── connections
│
├── docker-compose.yml
└── README.md
```

---

# Lancement du projet

## Démarrage des conteneurs Docker

```bash
docker compose up
```

---

# Accès Airflow

URL :

```text
http://localhost:8080
```

Identifiants :

```text
login : airflow
password : airflow
```

---

# Partie 1

Le fichier `intrusion.py` contient un DAG permettant :

- lecture du fichier de logs d'intrusion
- extraction des adresses IP
- correspondance IP / Pays via API
- génération d'un fichier CSV
- stockage des résultats

## DAG

```text
dag_intrusion
```

## Opérateurs utilisés

- PythonOperator
- PostgresOperator

---

# Partie 2

Le fichier `intrusion_2.py` contient plusieurs DAGs afin d'améliorer l'architecture du traitement.

## DAGs

### dag_intrusion_log

Responsabilités :

- lecture des logs
- extraction des IPs
- création du dataset d'inventaire

### dag_intrusion_pays

Responsabilités :

- enrichissement IP → Pays
- déclenchement automatique via Dataset Aware Scheduling

### dag_intrusion_db

Responsabilités :

- fusion des données
- génération du dataset final

---

# Technologies utilisées

- Apache Airflow
- Docker
- PostgreSQL
- Python
- Pandas

---

# Commandes Git importantes

## Partie 1

```bash
git commit -m "part_1 ..."
```

## Partie 2

```bash
git commit -m "part_2 ..."
```