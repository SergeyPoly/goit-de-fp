# Налаштування конфігурації SQL бази даних
db_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/",
    "db_olympic_dataset": "olympic_dataset",
    "db_spoly82": "spoly82",
    "table_bio": "athlete_bio",
    "table_events": "athlete_event_results",
    "table_result": "athlete_enriched_agg",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
}

# Налаштування конфігурації kafka
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Назви топіків
my_name = "serhii82"
athlete_event_results = f"{my_name}_athlete_event_results"
athlete_enriched_agg = f"{my_name}_athlete_enriched_agg"
