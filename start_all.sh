#!/bin/bash
echo "--- Uruchamianie Veturilo (macOS/Linux) ---"
docker compose up -d
sleep 10
python3 producers/producer_veturilo.py &
python3 producers/producer_weather.py &
echo "Projekt działa w tle!"