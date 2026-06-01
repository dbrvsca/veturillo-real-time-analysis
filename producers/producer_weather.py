import time
import requests
import json
from kafka import KafkaProducer

import os

# KONFIGURACJA
API_KEY = '1962c7c164f5abb7612f45d57631b086' # Darmowy klucz z OpenWeatherMap
CITY = 'Warsaw,pl'
KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = 'weather-raw'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"🌤️ Producent pogody działa. Wysyłam do: {TOPIC}")

while True:
    try:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric'
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()

        weather = {
            'temp': data['main']['temp'],
            'rain': data.get('rain', {}).get('1h', 0.0),
            'timestamp': int(time.time())
        }

        producer.send(TOPIC, value=weather)
        producer.flush()
        print(f"🌤️ Pogoda wysłana: {weather['temp']}°C, deszcz: {weather['rain']}mm")
    except Exception as e:
        print(f"❌ Błąd pogody: {e}")
        
    time.sleep(300) # Pogoda zmienia się wolniej, 5 minut wystarczy