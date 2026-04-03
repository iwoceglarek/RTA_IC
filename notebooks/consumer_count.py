from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

# Dla każdej wiadomości:
#   1. Zwiększ store_counts[store]
#   2. Dodaj amount do total_amount[store]
#   3. Co 10 wiadomości wypisz tabelę:
#      Sklep | Liczba | Suma | Średnia

for message in consumer:
    store = message.value["store"]
    
    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0) + message.value["amount"]
    msg_count += 1

    if msg_count % 10 == 0:
        print("\n=== PODSUMOWANIE SKLEPÓW ===")
        print("Sklep | Liczba | Suma | Średnia")
    
        for store in store_counts:
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count if count > 0 else 0
            print(f"{store} | {count} | {total:.2f} | {avg:.2f}")