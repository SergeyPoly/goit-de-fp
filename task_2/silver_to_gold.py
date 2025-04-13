from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, isnull, isnan

tables = ["athlete_bio", "athlete_event_results"]

# Ініціалізація Spark-сесії
spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()

# Зчитування даних Silver layer
athlete_bio_df = spark.read.parquet(f"silver/{tables[0]}")
athlete_event_results_df = spark.read.parquet(f"silver/{tables[1]}")

athlete_bio_df = athlete_bio_df.filter(
    (col("height") != '') &
    (~isnull("height")) & 
    (~isnan("weight")) & 
    (~isnull("weight"))
)

# Об’єднання даних
joined_df = athlete_event_results_df.join(athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc"), on="athlete_id", how="inner")

# Знаходження середнього зросту і ваги атлетів
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)

# Запис даних Gold layer у форматі Parquet
output_path = "gold/avg_stats"
aggregated_df.write.mode("overwrite").parquet(output_path)

print("Data successfully processed to Gold layer.")

# Зчитування даних Gold layer
df = spark.read.parquet(output_path)
df.show(truncate=False)

spark.stop()