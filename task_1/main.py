from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    isnan, 
    isnull,
    col,
    from_json,
    avg,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)
import os
from configs import db_config, kafka_config, athlete_event_results
from helpers import foreach_batch_function

# Пакет, необхідний для читання Kafka зі Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

athlete_results_schema = StructType(
    [
        StructField("edition", StringType(), True),
        StructField("edition_id", IntegerType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", IntegerType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("medal", StringType(), True),
        StructField("pos", StringType(), True),
        StructField("isTeamSport", StringType(), True),
    ]
)

# Створення Spark сесії
spark = SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar").appName("JDBCToKafka").getOrCreate()

# Етап 1: Зчитування даних фізичних показників атлетів
athlete_bio_df = spark.read.format("jdbc").options(
    url=db_config["url"] + db_config["db_olympic_dataset"],
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=db_config["table_bio"],
    user=db_config["user"],
    password=db_config["password"]).load()

# Етап 2: Фільтрація даних, де показники зросту та ваги є порожніми або не є числами
athlete_bio_df = athlete_bio_df.filter(
    (col("height") != '') &
    (~isnull("height")) & 
    (~isnan("weight")) & 
    (~isnull("weight")) 
)

# Етап 3.1: Зчитування даних з mysql таблиці athlete_event_results і запис в кафка топік
athlete_results_df = spark.read.format("jdbc").options(
    url=db_config["url"] + db_config["db_olympic_dataset"],
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=db_config["table_events"],
    user=db_config["user"],
    password=db_config["password"]).load()

athlete_results_json = athlete_results_df.selectExpr("athlete_id", "to_json(struct(*)) AS value")

(
athlete_results_json
    .write.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
            "kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
        )
    .option("topic", athlete_event_results)
    .save()
)

# Етап 3.2: Зчитування даних з результатами змагань з кафка топіку
athlete_results_streaming_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
            "kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
        )
    .option("subscribe", athlete_event_results)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as value")
    .select(from_json(col("value"), athlete_results_schema).alias("data"))
    .select("data.*")
)

# Етап 4: Об’єднання даних
joined_df = athlete_results_streaming_df.join(athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc"), on="athlete_id", how="inner")

# Етап 5: Знаходження середнього зросту і ваги атлетів
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)

# Етап 6. Cтрім даних (за допомогою функції forEachBatch)
query = aggregated_df.writeStream.foreachBatch(foreach_batch_function).outputMode("update").start().awaitTermination()