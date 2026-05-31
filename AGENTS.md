# AGENTS - Dokumentacja Techniczna Projektu: Real-Time Urban Mobility Monitor

## 1. Kontekst Projektu
System Veturilo Real-Time to rozproszona platforma do monitorowania dostępności rowerów miejskich w Warszawie. Projekt realizuje zadania inżynierii danych w czasie rzeczywistym, wykorzystując architekturę sterowaną zdarzeniami (event-driven architecture). Głównym celem jest predykcja braku dostępności rowerów oraz wizualizacja stanu floty na żywo.

## 2. Architektura Systemowa
System opiera się na kontenerach Dockerowych zintegrowanych w jednej sieci lokalnej:
- **Warstwa Ingestii (Producenci):** Skrypty Pythonowe pobierające dane z API Veturilo (status stacji) oraz API OpenWeather (warunki pogodowe).
- **Warstwa Kolejkowania (Broker):** Apache Kafka (topics: `veturilo-raw`, `weather-raw`, `veturilo-alerts`).
- **Warstwa Przetwarzania (Stream Engine):** Apache Spark (Structured Streaming). Odpowiada za:
    - Normalizację i czyszczenie danych.
    - Łączenie strumieni (Stream-Stream Join) z użyciem okien czasowych (Watermarking).
    - Agregację i wyliczanie KPI (wskaźnik zapełnienia stacji).
    - Logikę biznesową (filtrowanie stacji o zapełnieniu < 10%).
- **Warstwa SINK (Wyjście):**
    - PostgreSQL: Przechowywanie historycznego stanu dla BI.
    - Kafka `veturilo-alerts`: Sygnalizacja dla systemów zewnętrznych.
- **Warstwa Prezentacji:** Metabase (BI Dashboard) oraz mikroserwis alertujący (Discord/Telegram).

## 3. Standardy Kodowania i Konfiguracji
- **Format danych:** JSON.
- **Czas:** Timestamp Unix (UTC).
- **Infrastruktura:** Wszystkie usługi muszą być konfigurowalne przez `docker-compose.yml`. Każda usługa musi mieć własny wolumen (dane persistowane).
- **Obsługa błędów:** Producenci muszą implementować mechanizmy retry i logowanie błędów do standard error.
- **Konfiguracja środowiska:** Zależności Pythonowe definiowane są w `requirements.txt`. Używamy `pip install -r requirements.txt`.

## 4. Wytyczne dla AI (Jak masz mi pomagać)
Kiedy wklejam kod do analizy:
1. **Zawsze sprawdzaj poprawność schematów:** Weryfikuj, czy typy danych w Sparku zgadzają się z polami w JSONach z API.
2. **Dbaj o wydajność:** Sugeruj optymalizacje w operacjach join i grupowaniu (np. użycie `watermark` w Sparku).
3. **Pamiętaj o architekturze:** Jeśli poprawiasz skrypt, upewnij się, że nie narusza on komunikacji przez Kafkę (topic naming, serialization).
4. **Debugowanie:** Jeśli kod nie działa, analizuj go pod kątem potencjalnych błędów połączenia z Dockerem (`localhost:9092`) lub braku uprawnień w bazie PostgreSQL.
5. **Kontekstowy kod:** Pisząc funkcje, zakładaj strukturę folderów `producers/`, `stream_processing/`, `alerts/`.

## 5. Przepływ danych (Data Lineage)
1. API (Veturilo/Weather) -> 2. Producenci (Python) -> 3. Kafka Topics -> 4. Spark App (Logic) -> 5. PostgreSQL (Historian) LUB 6. Alert Consumer (Python) -> 7. Komunikator / Metabase.

## 6. Ograniczenia
Projekt działa w pełni lokalnie. Nie używamy żadnych usług chmurowych (AWS/GCP), co oznacza, że wszelkie połączenia sieciowe muszą być rozwiązywane przez nazwy kontenerów wewnątrz sieci Dockerowej (np. `postgres:5432`, `kafka:29092`).