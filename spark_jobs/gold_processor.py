"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    desc,
    current_timestamp,
    to_date
)

# ==========================================
# SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .appName("GoldLayerProcessor") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# LECTURE SILVER
# ==========================================

try:
    silver_df = spark.read.parquet(
        "s3a://silver/articles_clean/"
    )
    print("Lecture SILVER terminée")
except Exception as e:
    print("Erreur lors de la lecture de la couche SILVER :", str(e))
    spark.stop()
    exit(1)

# ==========================================
# ARTICLES PAR JOUR
# ==========================================

articles_par_jour = silver_df \
    .groupBy("date_parsed") \
    .agg(
        count("*").alias("nombre_articles")
    ) \
    .withColumn("processing_time", current_timestamp())

# ==========================================
# ARTICLES PAR SOURCE
# ==========================================

articles_par_source = silver_df \
    .groupBy("source") \
    .agg(
        count("*").alias("nombre_articles")
    ) \
    .withColumn("processing_time", current_timestamp())

# ==========================================
# TOP CATEGORIES
# ==========================================

top_categories = silver_df \
    .groupBy("categorie_clean") \
    .agg(
        count("*").alias("total")
    ) \
    .orderBy(desc("total")) \
    .withColumn("processing_time", current_timestamp())

# ==========================================
# ARTICLES PAR DATE + SOURCE
# ==========================================

articles_par_source_jour = silver_df \
    .groupBy(
        "source",
        "date_parsed"
    ) \
    .agg(
        count("*").alias("nombre_articles")
    ) \
    .withColumn("processing_time", current_timestamp())

# ==========================================
# JDBC CONFIG
# ==========================================

jdbc_url = "jdbc:postgresql://postgres:5432/warehouse"

jdbc_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# ==========================================
# WRITE TABLES
# ==========================================

articles_par_jour.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="articles_par_jour",
        properties=jdbc_properties
    )

print("Table articles_par_jour créée")

articles_par_source.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="articles_par_source",
        properties=jdbc_properties
    )

print("Table articles_par_source créée")

top_categories.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="top_categories",
        properties=jdbc_properties
    )

print("Table top_categories créée")

articles_par_source_jour.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="articles_par_source_jour",
        properties=jdbc_properties
    )

print("Table articles_par_source_jour créée")

# ==========================================
# FIN
# ==========================================

print("\n==============================")
print("GOLD LAYER TERMINÉ")
print("==============================")

spark.stop()"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, desc, current_timestamp, length, avg, 
    max, trim, round, hour, month, unix_timestamp, countDistinct
)

# ==========================================
# 1. INITIALISATION
# ==========================================
spark = SparkSession.builder \
    .appName("GoldLayer_10_Tables") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/warehouse"
jdbc_props = {"user": "admin", "password": "admin123", "driver": "org.postgresql.Driver"}

# ==========================================
# 2. CHARGEMENT ET PRÉPARATION
# ==========================================
# On utilise .persist() car on va déclencher 10 actions d'écriture
silver_df = spark.read.parquet("s3a://silver/articles_clean/") \
    .filter((col("categorie_clean").isNotNull()) & (trim(col("categorie_clean")) != "")) \
    .persist()

# ==========================================
# 3. CRÉATION DES 10 TABLES
# ==========================================

# T1: Volume par jour
t1 = silver_df.groupBy("date_parsed").agg(count("*").alias("nb_articles"))

# T2: Volume par source
t2 = silver_df.groupBy("source").agg(count("*").alias("nb_articles"))

# T3: Top Catégories
t3 = silver_df.groupBy("categorie_clean").agg(count("*").alias("total")).orderBy(desc("total"))

# T4: Longueur des textes par catégorie
t4 = silver_df.withColumn("len", length("contenu_clean")) \
    .groupBy("categorie_clean").agg(round(avg("len"), 0).alias("avg_len"), max("len").alias("max_len"))

# T5: Top 20 Auteurs
t5 = silver_df.groupBy("auteur_clean").agg(count("*").alias("nb_publis")).orderBy(desc("nb_publis")).limit(20)

# T6: Matrice Source/Catégorie (Répartition)
t6 = silver_df.groupBy("source", "categorie_clean").agg(count("*").alias("volume"))

# T7: Analyse de la latence (Délai d'ingestion en minutes)
t7 = silver_df.withColumn("delay_min", 
    (unix_timestamp("ingestion_time") - unix_timestamp("date_parsed")) / 60) \
    .groupBy("source").agg(round(avg("delay_min"), 2).alias("latence_moyenne_min"))

# T8: Pic d'activité horaire (Basé sur l'ingestion)
t8 = silver_df.withColumn("heure", hour("ingestion_time")) \
    .groupBy("heure").agg(count("*").alias("articles_recus")).orderBy("heure")

# T9: Diversité des sources (Nombre de catégories par source)
t9 = silver_df.groupBy("source").agg(countDistinct("categorie_clean").alias("nb_categories_couvertes"))

# T10: Bilan mensuel
t10 = silver_df.withColumn("mois", month("date_parsed")) \
    .groupBy("mois").agg(count("*").alias("total_mensuel")).orderBy("mois")

# ==========================================
# 4. EXPORTATION AUTOMATISÉE
# ==========================================
all_tables = {
    "gold_vol_jour": t1, "gold_vol_source": t2, "gold_top_cat": t3,
    "gold_len_cat": t4, "gold_top_aut": t5, "gold_matrice_src_cat": t6,
    "gold_latence": t7, "gold_pics_horaires": t8, "gold_diversite": t9, "gold_mensuel": t10
}

for name, df in all_tables.items():
    df.withColumn("updated_at", current_timestamp()) \
      .write.mode("overwrite").jdbc(url=jdbc_url, table=name, properties=jdbc_props)
    print(f"Table {name} enregistrée.")

silver_df.unpersist()
spark.stop()