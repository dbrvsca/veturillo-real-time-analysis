import time
import requests
import json
from kafka import KafkaProducer

import os

# Adres Kafki
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'veturilo-raw'

# Inicjalizacja producenta
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producent Veturilo wystartował. Trwa wysyłanie danych do Kafki ({KAFKA_BOOTSTRAP_SERVERS})")


while True:
    try:
        response = requests.get('https://nextbike.net/maps/nextbike-live.json?city=812')
        response.raise_for_status()
        data = response.json()
        places = data['countries'][0]['cities'][0]['places']

        for place in places:
            station_event = {
                'station_id': int(place.get('uid')) if place.get('uid') is not None else None,
                'name': place.get('name'),
                'bikes_available': int(place.get('bikes', 0)),
                'bike_racks': int(place.get('bike_racks', 0)),
                'free_racks': int(place.get('free_racks', 0)),
                'timestamp': int(time.time())
            }
            producer.send(TOPIC_NAME, value=station_event)
        
        producer.flush()
        print(f"Wysłałem dane o {len(places)} stacjach.")
    except Exception as e:
        print(f"Błąd: {e}")
    
    time.sleep(60) # Czekamy minutę przed kolejnym odczytem