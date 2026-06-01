import os
import json
import requests
from kafka import KafkaConsumer

# 1. Konfiguracja (DevOps / Env Variables)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "veturilo-alerts"

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

print("-----------------------------------------------------------------")
print("🤖 Uruchamianie Alert Bota Veturilo...")
print(f"Subskrypcja topica Kafka: {TOPIC_NAME} na {KAFKA_BOOTSTRAP_SERVERS}")
if DISCORD_WEBHOOK_URL:
    print("📢 Integracja z Discordem: WŁĄCZONA")
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    print("📢 Integracja z Telegramem: WŁĄCZONA")
if not DISCORD_WEBHOOK_URL and not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
    print("📢 Tryb: Tylko konsola (ustaw DISCORD_WEBHOOK_URL lub TELEGRAM_BOT_TOKEN, aby włączyć powiadomienia)")
print("-----------------------------------------------------------------")

# 2. Inicjalizacja Konsumenta Kafki
try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='veturilo-alert-bot-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
except Exception as e:
    print(f"❌ Błąd krytyczny połączenia z Kafką: {e}")
    exit(1)

def send_discord_alert(alert_data):
    if not DISCORD_WEBHOOK_URL:
        return
    
    station_name = alert_data.get('name', 'Nieznana')
    bikes = alert_data.get('bikes_available', 0)
    capacity = alert_data.get('capacity', 10)
    rate = alert_data.get('occupancy_rate', 0.0)
    temp = alert_data.get('temp', 20.0)
    rain = alert_data.get('rain', 0.0)
    
    # Tworzymy elegancki format Embed dla Discorda
    payload = {
        "embeds": [
            {
                "title": "🚨 ALARM: Krytyczny Brak Rowerów! 🚨",
                "description": f"Stacja **{station_name}** jest prawie pusta!",
                "color": 15158332, # Czerwony
                "fields": [
                    {
                        "name": "Dostępne rowery",
                        "value": f"🚲 **{bikes}** / {capacity} stojaków",
                        "inline": True
                    },
                    {
                        "name": "Stopień zapełnienia",
                        "value": f"📈 **{rate:.1f}%**",
                        "inline": True
                    },
                    {
                        "name": "Warunki pogodowe",
                        "value": f"🌡️ **{temp:.1f}°C** | 🌧️ **{rain:.1f} mm** deszczu",
                        "inline": False
                    }
                ],
                "footer": {
                    "text": "System Monitorowania Veturilo w Czasie Rzeczywistym"
                }
            }
        ]
    }
    
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Błąd wysyłania do Discorda: {e}")

def send_telegram_alert(alert_data):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
        
    station_name = alert_data.get('name', 'Nieznana')
    bikes = alert_data.get('bikes_available', 0)
    capacity = alert_data.get('capacity', 10)
    rate = alert_data.get('occupancy_rate', 0.0)
    temp = alert_data.get('temp', 20.0)
    rain = alert_data.get('rain', 0.0)
    
    text = (
        f"🚨 *ALARM: Krytyczny Brak Rowerów!*\n\n"
        f"Stacja: *{station_name}*\n"
        f"🚲 Dostępne rowery: *{bikes}* / {capacity} stojaków\n"
        f"📈 Stopień zapełnienia: *{rate:.1f}%*\n"
        f"🌡️ Temperatura: *{temp:.1f}°C*\n"
        f"🌧️ Deszcz: *{rain:.1f} mm*\n\n"
        f"_Monitor Veturilo Real-Time_"
    )
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    
    try:
        r = requests.post(url, json=payload)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Błąd wysyłania do Telegrama: {e}")

# 3. Pętla konsumencka Kafki
print("🤖 Bot czeka na alerty z Kafki...")
for message in consumer:
    alert = message.value
    station_name = alert.get('name', 'Nieznana')
    bikes = alert.get('bikes_available', 0)
    capacity = alert.get('capacity', 10)
    rate = alert.get('occupancy_rate', 0.0)
    temp = alert.get('temp', 20.0)
    rain = alert.get('rain', 0.0)
    
    # 1. Wyświetlamy alert w konsoli z ładną oprawą graficzną
    print("\n" + "="*60)
    print(f"🚨 ALERT: STACJA KRYTYCZNIE PUSTA! 🚨")
    print(f"📍 Stacja:          {station_name} (ID: {alert.get('station_id')})")
    print(f"🚲 Rowery:          {bikes} z {capacity} dostępnych ({rate:.1f}% zapełnienia)")
    print(f"🌤️ Warunki:         {temp:.1f}°C, Deszcz: {rain:.1f} mm")
    print("="*60 + "\n")
    
    # 2. Wysyłamy powiadomienie przez Discord Webhook jeśli jest skonfigurowane
    send_discord_alert(alert)
    
    # 3. Wysyłamy powiadomienie przez Telegram jeśli jest skonfigurowane
    send_telegram_alert(alert)
