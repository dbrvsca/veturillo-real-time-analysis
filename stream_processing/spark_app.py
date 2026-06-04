import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_json, struct, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType

# 1. Konfiguracja połączeń (DevOps / Env Variables)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "veturilo_db")
DB_USER = os.environ.get("DB_USER", "veturilo_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "veturilo_password")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

print("-----------------------------------------------------------------")
print(f"Uruchamianie aplikacji PySpark...")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"PostgreSQL: {JDBC_URL}")
print("-----------------------------------------------------------------")

# 2. Inicjalizacja Sesji Spark z wymaganymi pakietami Maven
# Używamy pakietów dla Sparka w wersji 3.5.0 (dopasowane do domyślnych w środowisku)
spark = SparkSession.builder \
    .appName("VeturiloRealTimeAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Definicja schematów danych z Kafki
# Schemat dla Veturilo
veturilo_schema = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("bikes_available", IntegerType(), True),
    StructField("bike_racks", IntegerType(), True),
    StructField("free_racks", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Schemat dla Pogody
weather_schema = StructType([
    StructField("temp", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# 4. Odczyt strumieni z Kafki
# Strumień Veturilo
veturilo_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "veturilo-raw") \
    .option("startingOffsets", "latest") \
    .load()

# Strumień Pogody
weather_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "weather-raw") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Przetwarzanie i normalizacja danych wejściowych
# Dekodowanie JSON i konwersja czasu do TimestampType (wymagane do Watermarka)
veturilo_df = veturilo_raw \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json("json_payload", veturilo_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType())) \
    .withWatermark("event_time", "10 minutes")

weather_df = weather_raw \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json("json_payload", weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("weather_time", from_unixtime(col("timestamp")).cast(TimestampType())) \
    .withWatermark("weather_time", "10 minutes")

# 6. Stream-Stream Join z oknem czasowym (Watermarking Join)
# Łączymy dane o stacjach rowerowych z najbliższym odczytem pogodowym (+/- 15 minut)
joined_df = veturilo_df.join(
    weather_df,
    expr("""
        event_time >= weather_time - interval 15 minutes AND
        event_time <= weather_time + interval 15 minutes
    """),
    "inner"
)

# 7. Obliczanie wskaźników KPI (Zapełnienie stacji)
# Capacity to suma dostępnych rowerów i wolnych stojaków. Jeśli oba są 0, używamy bike_racks, minimalnie 10.
processed_df = joined_df.withColumn(
    "capacity",
    when((col("bikes_available") + col("free_racks")) > 0, col("bikes_available") + col("free_racks"))
    .otherwise(when(col("bike_racks") > 0, col("bike_racks")).otherwise(10))
).withColumn(
    "occupancy_rate",
    (col("bikes_available") / col("capacity")) * 100.0
).select(
    col("station_id"),
    col("name"),
    col("bikes_available"),
    col("bike_racks"),
    col("free_racks"),
    col("occupancy_rate"),
    col("temp"),
    col("rain"),
    col("event_time")
)

# 8. Zdefiniowanie zapisu ForeachBatch
# Pozwala to na jednoczesny zapis do bazy PostgreSQL oraz wysyłkę alertów do Kafki
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
        
    print(f"---> Przetwarzanie micro-batcha {batch_id} ({batch_df.count()} rekordów)")
    
    # A. Zapis pełnych danych historycznych do Postgresa
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "station_status") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"     [PostgreSQL] Pomyślnie zapisano do station_status.")
    except Exception as e:
        print(f"     [PostgreSQL] Błąd zapisu do station_status: {e}", file=sys.stderr)

    # B. Filtrowanie i obsługa alertów (< 10% zapełnienia stacji)
    alerts_df = batch_df.filter(col("occupancy_rate") < 10.0)
    
    if alerts_df.count() > 0:
        print(f"     [Alert] Znaleziono {alerts_df.count()} stacji o krytycznym zapełnieniu (< 10%)!")
        
        # B1. Zapis alertów do Postgresa (dla historii)
        try:
            alerts_df.write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", "veturilo_alerts") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"     [PostgreSQL] Pomyślnie zapisano alerty do veturilo_alerts.")
        except Exception as e:
            print(f"     [PostgreSQL] Błąd zapisu alertów do veturilo_alerts: {e}", file=sys.stderr)
            
        # B2. Wysyłka alertów z powrotem na Kafkę (topic: veturilo-alerts) dla Alert Bota
        try:
            alerts_kafka_df = alerts_df.selectExpr(
                "CAST(station_id AS STRING) AS key",
                "to_json(struct(*)) AS value"
            )
            alerts_kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", "veturilo-alerts") \
                .save()
            print(f"     [Kafka] Alerty pomyślnie wysłane do topica veturilo-alerts.")
        except Exception as e:
            print(f"     [Kafka] Błąd wysyłania alertów do topica veturilo-alerts: {e}", file=sys.stderr)


# 9. Uruchomienie strumienia zapisu
query = processed_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Spark Streaming wystartował pomyślnie. Oczekiwanie na dane ze strumieni...")
query.awaitTermination()
