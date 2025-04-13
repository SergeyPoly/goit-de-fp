from pyspark.sql import SparkSession
import requests

tables = ["athlete_bio", "athlete_event_results"]

def download_data(table_name):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + table_name + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(table_name + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {table_name}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


# Ініціалізація Spark-сесії
spark = SparkSession.builder.appName("landing_to_Bronze").getOrCreate()

for table in tables:
    output_path = f"bronze/{table}"
    
    download_data(table)

    # Зчитування попередньо завантаженого CSV файлу за допомогою Spark
    df = spark.read.option("header", "true").csv(table + ".csv")
    df.show()

    # Запис даних Bronze layer у форматі Parquet 
    df.write.mode("overwrite").parquet(output_path)

print("Data successfully processed to Bronze layer.")
spark.stop()