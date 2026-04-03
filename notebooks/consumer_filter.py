from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

# Dla każdej wiadomości: sprawdź czy amount > 3000, jeśli tak — wypisz ALERT
# Format: ALERT: TX0042 | 2345.67 PLN | Warszawa | elektronika
for message in consumer:
    if(message.value['amount']>3000):
        print("ALERT", message.value['tx_id'], "|", message.value['amount'], "|",
              message.value['store'], "|", message.value['category'],)
