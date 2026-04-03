from kafka import KafkaConsumer
import json

# Czytaj z 'transactions' (użyj INNEGO group_id!)
# Dodaj pole risk_level na podstawie amount
# Wypisz wzbogaconą transakcję
consumer = KafkaConsumer(
'transactions',
bootstrap_servers='broker:9092',
group_id='consumer-enrich-group',  # inny group_id niż w poprzednim zadaniu
value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    amount = message.value['amount']
    
    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    message.value['risk_level'] = risk_level
    print(message.value)
