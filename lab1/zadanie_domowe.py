from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Napisz konsumenta wykrywającego anomalie prędkości: alert jeśli ten sam user_id wykona więcej niż 3 transakcje w ciągu 60 sekund.
user_events = dict()
for message in consumer:
    user_id = message.value['user_id']
    timestamp = datetime.fromisoformat(message.value['timestamp']).timestamp()

    if user_id not in user_events:
        user_events[user_id] = []
    
    user_events[user_id].append(timestamp)

    # Ususwanie starych transakcji
    user_events[user_id] = [
        t for t in user_events[user_id]
        if timestamp - t <= 60
    ]

    # Sprawdzenie progu
    if len(user_events[user_id]) > 3:
        print(f"ALERT: user {user_id} wykonał {len(user_events[user_id])} transakcji w 60s")
                
    

