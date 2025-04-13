from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

tables = ["athlete_bio", "athlete_event_results"]

# Ініціалізація Spark-сесії
spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()

# Функція очищення тексту
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())


for table in tables:
    input_path = f"bronze/{table}"
    output_path = f"silver/{table}"

    # Зчитування даних Bronze layer
    df = spark.read.parquet(input_path)

    # Обробка текстових колонок
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            print(f"        Cleaning column: {col_name}")
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    # Видалення дублікатів
    df = df.dropDuplicates()
    df.show()

    # Запис даних Silver layer у форматі Parquet 
    df.write.mode("overwrite").parquet(output_path)

print("Data successfully processed to Silver layer.")
spark.stop()