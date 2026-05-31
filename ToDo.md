# TODO List - Zadania Projektowe

## 1. Data Engineer (Źródła i API)
- **Zadanie:** Implementacja producentów danych (Python).
- **Cel:** Pobieranie danych z API Veturilo i OpenWeather w pętli `while True` i wysyłka do Kafki.
- **Pliki:** - `producers/producer_veturilo.py`
  - `producers/producer_weather.py`

## 2. Spark Engineer (Inicjalizacja i Schematy)
- **Zadanie:** Konfiguracja PySpark i połączenie z Kafką.
- **Cel:** Ustawienie sesji Spark, definicja schematów (StructType) i `withWatermark`.
- **Pliki:** - `stream_processing/spark_app.py`

## 3. Data Processing Engineer (Logika Biznesowa)
- **Zadanie:** Przetwarzanie i transformacje danych.
- **Cel:** Join strumieni (Rower + Pogoda), obliczenie % zapełnienia, filtracja alertów (< 10%) i zapis do bazy.
- **Pliki:** - `stream_processing/spark_app.py`

## 4. DevOps / Infra Engineer (Infrastruktura i Środowisko)
- **Zadanie:** Utrzymanie stabilności całego systemu.
- **Cel:** Optymalizacja Docker Compose, konfiguracja sieci, volumes dla bazy danych i persystencja Metabase.
- **Pliki:** - `docker-compose.yml`
  - `README.md`

## 5. Backend Developer (Alertowanie)
- **Zadanie:** System powiadomień.
- **Cel:** Konsumpcja topicu `veturilo-alerts`, sformatowanie wiadomości i wysyłka przez Webhook na Discord/Telegram.
- **Pliki:** - `alerts/alert_bot.py`

## 6. BI Analyst (Wizualizacja)
- **Zadanie:** Analiza i dashboardy.
- **Cel:** Konfiguracja Metabase, SQL-owa warstwa semantyczna, budowa dashboardu z mapą stacji.
- **Pliki:** - Panel Metabase (GUI)
  - Folder `scripts/` (pliki z zapytaniami SQL)