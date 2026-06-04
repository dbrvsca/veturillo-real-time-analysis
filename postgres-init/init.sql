-- Inicjalizacja bazy danych Veturilo
CREATE TABLE IF NOT EXISTS station_status (
    id SERIAL PRIMARY KEY,
    station_id INT,
    name VARCHAR(255),
    bikes_available INT,
    bike_racks INT,
    free_racks INT,
    occupancy_rate DOUBLE PRECISION,
    temp DOUBLE PRECISION,
    rain DOUBLE PRECISION,
    event_time TIMESTAMP WITH TIME ZONE,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS veturilo_alerts (
    id SERIAL PRIMARY KEY,
    station_id INT,
    name VARCHAR(255),
    bikes_available INT,
    bike_racks INT,
    occupancy_rate DOUBLE PRECISION,
    temp DOUBLE PRECISION,
    rain DOUBLE PRECISION,
    event_time TIMESTAMP WITH TIME ZONE,
    sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indeksy dla lepszej wydajności zapytań analitycznych w Metabase
CREATE INDEX IF NOT EXISTS idx_station_status_event_time ON station_status(event_time);
CREATE INDEX IF NOT EXISTS idx_station_status_station_id ON station_status(station_id);
CREATE INDEX IF NOT EXISTS idx_veturilo_alerts_event_time ON veturilo_alerts(event_time);
