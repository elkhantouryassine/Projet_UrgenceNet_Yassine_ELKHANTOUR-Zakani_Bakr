<<<<<<< HEAD
# UrgenceNet — Jalon 1

Pipeline Big Data pour un réseau hospitalier fictif. Ce guide permet de lancer le cluster, de traiter l’historique avec les RDD et de produire le Parquet Silver avec Spark SQL et les DataFrames.

## Prérequis et fichiers du dépôt

Utiliser Docker Desktop démarré avec les conteneurs Linux, Docker Compose v2 et un terminal PowerShell ouvert à la racine du dépôt. Le premier lancement télécharge les images Docker, JupyterLab et le connecteur S3A depuis Maven. Java et Python sont fournis par les conteneurs.

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` | Kafka KRaft, Spark Master, deux Workers, MinIO et Jupyter |
| `Dockerfile.jupyter` | Image JupyterLab basée sur Spark, avec un dossier personnel accessible à l’utilisateur `spark` |
| `notebooks/01_cluster_check.ipynb` | Vérification du cluster |
| `notebooks/producer.py` | Générateur de l’historique et du référentiel |
| `notebooks/02_batch_rdd.ipynb` | Analyse batch avec SparkCore |
| `notebooks/03_batch_dataframe_sql.ipynb` | Analyse DataFrame et SQL, nettoyage et export Silver |

Configuration du projet — Spark **3.5.7**, Scala **2.12**, Java **17**, JupyterLab **4.4.10**, Kafka **3.9.1**, MinIO Community `dappros/minio:RELEASE.2025-09-07T16-13-09Z`. Le connecteur `org.apache.hadoop:hadoop-aws:3.3.4` correspond à Hadoop 3.3.4. Si l’image Spark change, vérifier le JAR `hadoop-client-api` avant de modifier la dépendance.

## Partie 1 — Démarrer et vérifier le cluster

Dans PowerShell, depuis la racine du dépôt :

```powershell
docker compose version
docker compose -f .\docker-compose.yml config --quiet
docker compose -f .\docker-compose.yml up -d --build
docker compose -f .\docker-compose.yml ps -a
```

Les six services doivent être `Up`. Ouvrir [Spark Master](http://localhost:8080) et vérifier que les deux Workers sont `ALIVE`. Prendre une capture pour le rapport.

Créer puis décrire le topic Kafka :

```powershell
docker compose -f .\docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic urgences --partitions 3 --replication-factor 1
docker compose -f .\docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic urgences
```

La sortie doit indiquer `PartitionCount: 3`. Le facteur de réplication vaut 1 pour le broker local.

Ouvrir [MinIO](http://localhost:9001), se connecter avec les identifiants locaux configurés dans le service `minio`, puis cliquer sur **Create Bucket** et créer **`lake`** s’il n’existe pas. Le guide utilise l’utilisateur `urgenceadmin` et le mot de passe de démonstration `urgence-local-2026`.

Ouvrir [JupyterLab](http://localhost:8888/lab?token=urgencenet-local) avec le jeton local du Compose. Exécuter toutes les cellules de `01_cluster_check.ipynb` de haut en bas. La SparkSession doit afficher `spark://spark-master:7077` comme Master, Spark `3.5.7` et la somme **5050** pour le RDD de deux partitions. Sauvegarder, puis arrêter le kernel du notebook avant d’ouvrir la partie 2 pour libérer les cœurs des Workers.

## Partie 2 — Générer puis analyser l’historique avec les RDD

Dans PowerShell :

```powershell
docker compose -f .\docker-compose.yml exec jupyter python3 /workspace/producer.py --mode file --n 50000
Get-ChildItem .\notebooks\data
```

Le générateur du projet produit **50 000 événements**, **12 250 passages distincts**, **1 000 lignes dupliquées** et un référentiel de **40 établissements**.

Dans le navigateur d’objets de MinIO, ouvrir `lake`, créer le chemin `bronze/history/` et y téléverser `notebooks/data/urgences_history.jsonl`. Vérifier la destination exacte :

```text
s3a://lake/bronze/history/urgences_history.jsonl
```

Dans JupyterLab, ouvrir `02_batch_rdd.ipynb` et exécuter ses cellules dans l’ordre. La première cellule charge le connecteur S3A et utilise l’API MinIO à `minio:9000` sur le réseau Docker. Les identifiants du notebook doivent correspondre à ceux du Compose. La lecture via `sc.textFile()` doit retrouver **50 000 lignes**.

Vérifier le tableau du nombre de prises en charge et du délai moyen pour les quatre villes, les cinq niveaux de priorité et le top 5 des établissements. Les calculs utilisent l’historique brut, doublons inclus. Dans le lineage, repérer `ShuffledRDD` pour expliquer le shuffle de `reduceByKey`.

Avant d’arrêter le kernel, ouvrir [Spark UI → Jobs](http://localhost:4040/jobs/), sélectionner le job de l’agrégation par ville, capturer son DAG et relever le nombre de stages indiqué dans l’interface. Sauvegarder puis arrêter le kernel.

## Partie 3 — Exécuter Spark SQL et produire le Parquet Silver

Dans MinIO, créer le chemin `bronze/reference/` dans le bucket `lake`. Y téléverser `notebooks/data/facilities.json`. Le notebook attend le chemin suivant :

```text
s3a://lake/bronze/reference/facilities.json
```

Ouvrir `03_batch_dataframe_sql.ipynb` et exécuter ses cellules dans l’ordre. Le fichier du générateur contient un objet JSON par ligne. Si le référentiel fourni est un tableau JSON, activer l’option `multiLine` sur la lecture du référentiel.

| Vérification | Résultat attendu avec les données du projet |
|---|---|
| Lecture avec des schémas explicites | 50 000 événements et 40 établissements |
| Agrégation par ville | Résultats identiques en RDD, DataFrame et SQL pour le même historique brut |
| Jointure par établissement | Prises en charge et attente moyenne par ville et type, plan physique `BroadcastHashJoin` |
| UDF `wait_band` et expression `when` | 0 différence entre les catégories |
| Dédoublonnage par `visit_id` et `status` | 50 000 → 49 000 lignes |
| Auto-jointure entre arrivée et prise en charge | 12 250 délais contrôlés, 0 incohérence |
| Relecture Parquet | 49 000 lignes réparties dans quatre partitions `city=...` |

Le notebook écrit dans `s3a://lake/silver/urgences_history/`. Relancer la cellule d’écriture remplace cette sortie Silver. Vérifier la présence des quatre partitions dans MinIO. Conserver les captures du DAG et des stages de la jointure, ainsi que la sortie `explain(True)`. Relever les temps réellement mesurés pour le calcul RDD et DataFrame sur le même historique brut.

## Dépannage

| Symptôme | Vérification |
|---|---|
| Conflit de nom de conteneur | Vérifier les lignes `container_name:` dans la configuration effective avec `docker compose -f .\docker-compose.yml config`. Le nom du service `spark-master` suffit pour le réseau interne. |
| MinIO affiche `pull access denied` | Vérifier la référence MinIO dans le Compose, puis tester `docker compose -f .\docker-compose.yml pull minio`. |
| Jupyter affiche `Permission denied: '/nonexistent'` | Vérifier que le Dockerfile crée `/home/spark` avec `spark` comme propriétaire et définit `HOME=/home/spark`. Recréer Jupyter avec `docker compose -f .\docker-compose.yml up -d --build --no-deps jupyter`. |
| Spark attend des ressources ou perd un Worker | Vérifier les deux Workers dans Spark Master et arrêter les kernels des notebooks précédents. Consulter les logs des Workers si l’un d’eux est absent. |
| Spark signale `Input path does not exist` | Vérifier le nom de l’objet et son chemin dans le bucket `lake`. |
| `SparkContext` ne reconnaît pas `clearJobGroup()` | Effacer le groupe avec `sc.setLocalProperty(..., None)` pour les clés `spark.jobGroup.id`, `spark.job.description` et `spark.job.interruptOnCancel`. |

Afficher les logs Jupyter ou arrêter les services depuis PowerShell :

```powershell
docker compose -f .\docker-compose.yml logs --tail 60 jupyter
docker compose -f .\docker-compose.yml stop
```

Relancer les services avec `docker compose -f .\docker-compose.yml up -d`. Les données MinIO sont stockées dans le volume nommé `minio-data`.
=======
# Projet_UrgenceNet_Yassine_ELKHANTOUR-Zakani_Bakr
>>>>>>> origin/main
