# TODO List - Zadania Projektowe

## 1. Data Engineer (Źródła i API)
- [x] **Zadanie:** Implementacja producentów danych (Python).
- [x] **Cel:** Pobieranie danych z API Veturilo i OpenWeather w pętli `while True` i wysyłka do Kafki.
- [x] **Pliki:** 
  - [x] `producers/producer_veturilo.py` (rozszerzony o wysyłanie rack_locks / free_racks i dynamiczny bootstrap)
  - [x] `producers/producer_weather.py` (dynamiczny bootstrap)

## 2. Spark Engineer (Inicjalizacja i Schematy)
- [x] **Zadanie:** Konfiguracja PySpark i połączenie z Kafką.
- [x] **Cel:** Ustawienie sesji Spark, definicja schematów (StructType) i `withWatermark`.
- [x] **Pliki:** 
  - [x] `stream_processing/spark_app.py`

## 3. Data Processing Engineer (Logika Biznesowa)
- [x] **Zadanie:** Przetwarzanie i transformacje danych.
- [x] **Cel:** Join strumieni (Rower + Pogoda), obliczenie % zapełnienia, filtracja alertów (< 10%) i zapis do bazy.
- [x] **Pliki:** 
  - [x] `stream_processing/spark_app.py`

## 4. DevOps / Infra Engineer (Infrastruktura i Środowisko)
- [x] **Zadanie:** Utrzymanie stabilności całego systemu.
- [x] **Cel:** Optymalizacja Docker Compose, konfiguracja sieci, volumes dla bazy danych i persystencja Metabase.
- [x] **Zrealizowano:** Dodanie Metabase do sieci Docker, trwała persystencja wolumenów (Postgres + Kafka + Metabase), automatyczne tworzenie schematu tabel (`postgres-init/init.sql`), dodanie systemowych healthchecków.
- [x] **Pliki:** 
  - [x] `docker-compose.yml`
  - [x] `README.md`

## 5. Backend Developer (Alertowanie)
- [x] **Zadanie:** System powiadomień.
- [x] **Cel:** Konsumpcja topicu `veturilo-alerts`, sformatowanie wiadomości i wysyłka przez Webhook na Discord/Telegram.
- [x] **Pliki:** 
  - [x] `alerts/alert_bot.py`

## 6. BI Analyst (Wizualizacja)
- [x] **Zadanie:** Analiza i dashboardy.
- [x] **Cel:** Konfiguracja Metabase, SQL-owa warstwa semantyczna, budowa dashboardu z mapą stacji.
- [x] **Pliki:** 
  - [x] Panel Metabase (konfiguracja gotowa w sieci Docker)
  - [x] Folder `scripts/` (pliki z zapytaniami SQL korelującymi pogodę i obłożenie)