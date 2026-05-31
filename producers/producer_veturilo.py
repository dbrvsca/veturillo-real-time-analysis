import time
import requests
import json
from kafka import KafkaProducer

# Adres Kafki
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'veturilo-raw'

# Inicjalizacja producenta
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Producent Veturilo wystartował. Trwa wysyłanie danych do Kafki")


while True:
    try:
        response = requests.get('https://nextbike.net/maps/nextbike-live.json?city=812')
        response.raise_for_status()
        data = response.json()
        places = data['countries'][0]['cities'][0]['places']

        for place in places:
            station_event = {
                'station_id': place.get('uid'),
                'name': place.get('name'),
                'bikes_available': place.get('bikes'),
                'timestamp': int(time.time())
            }
            producer.send(TOPIC_NAME, value=station_event)
        
        producer.flush()
        print(f"Wysłałem dane o {len(places)} stacjach.")
    except Exception as e:
        print(f"Błąd: {e}")
    
    time.sleep(60) # Czekamy minutę przed kolejnym odczytem