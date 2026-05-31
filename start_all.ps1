Write-Host "--- Uruchamianie Veturilo (Windows) ---"
docker compose up -d
Start-Sleep -Seconds 10
Start-Process python -ArgumentList "producers/producer_veturilo.py"
Start-Process python -ArgumentList "producers/producer_weather.py"
Write-Host "Projekt działa w tle!"