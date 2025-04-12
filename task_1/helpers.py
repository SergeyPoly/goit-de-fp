from configs import kafka_config, athlete_enriched_agg, db_config

# 6a). стрім у вихідний кафка-топік
def write_to_kafka(df, epoch_id):
    try:
        (
        df.selectExpr("to_json(struct(*)) AS value")
            .write.format("kafka")
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
            .option("kafka.security.protocol", kafka_config["security_protocol"])
            .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
            .option(
                "kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
            )
            .option("topic", athlete_enriched_agg)
            .save()
        )

    except Exception as e:
        print(f"Error writing batch to Kafka: {e}")


# 6b). стрім у базу даних
def write_to_database(df, epoch_id): 
    try:
        (
        df.write
            .mode("append")
            .format("jdbc")
            .options(
                url=db_config["url"] + db_config["db_spoly82"],
                driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
                dbtable=db_config["table_result"],
                user=db_config["user"],
                password=db_config["password"]
            ).save()
        )
    
    except Exception as e:
        print(f"Error writing batch to MySQL: {e}")


def foreach_batch_function(batch_df, epoch_id):
    write_to_kafka(batch_df, epoch_id)
    write_to_database(batch_df, epoch_id)