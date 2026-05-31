# Veturilo Real-Time Analysis

## Start Projektu
Wybierz instrukcję dla swojego systemu:

### Windows
1. Otwórz PowerShell w folderze projektu.
2. Odblokuj uprawnienia: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process`
3. Uruchom: `.\start_all.ps1`
4. Wpisz komende `docker ps`
5. Jeśli wszystkie serwisy mają status up możesz zaczynać pracę, jeśli nie patrz punkt 6.
6. chatgpt.com
### macOS / Linux
1. Otwórz terminal w folderze projektu.
2. Nadaj uprawnienia: `chmod +x start_all.sh`
3. Uruchom: `./start_all.sh`
4. Wpisz komende `docker ps`
5. Jeśli wszystkie serwisy mają status up możesz zaczynać pracę, jeśli nie patrz punkt 6.
6. chatgpt.com

## Architektura i Role
Szczegółowy podział zadań znajdziesz w pliku `ToDo.md`.
Szczegółowy opis projektu jest w pliku `AGENTS.md`.

## Dane techniczne
- **Kafka:** `localhost:9092`
- **Baza:** `localhost:5432` (user: veturilo_user / pass: veturilo_password)
- **Metabase:** Dostępny na `http://localhost:3000`

## Jak pobrać projekt?
1. `git clone https://github.com/TWOJA_NAZWA/veturillo-real-time-analysis.git`
2. `cd veturillo-real-time-analysis`
3. Dalej postępuj zgodnie z instrukcją startową (uruchomienie Docker Desktop + skrypt startowy).
