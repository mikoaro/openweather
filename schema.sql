-- Remove the table if it already exists (useful for testing)
DROP TABLE IF EXISTS weather_metrics;

-- Create the climate metrics table
CREATE TABLE weather_metrics (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    temperature_celsius FLOAT NOT NULL,
    humidity INTEGER NOT NULL,
    pressure INTEGER NOT NULL,
    wind_speed FLOAT NOT NULL,
    timestamp_unix BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes to optimize queries
CREATE INDEX idx_city ON weather_metrics(city);
CREATE INDEX idx_timestamp ON weather_metrics(timestamp_unix);
CREATE INDEX idx_created_at ON weather_metrics(created_at);

-- Comments in the columns (documentation)
COMMENT ON TABLE weather_metrics IS 'Meteorological metrics of United States capitals';
COMMENT ON COLUMN weather_metrics.city IS 'City name';
COMMENT ON COLUMN weather_metrics.temperature_celsius IS 'Temperature in degrees Celsius';
COMMENT ON COLUMN weather_metrics.humidity IS 'Relative humidity of the air (%)';
COMMENT ON COLUMN weather_metrics.pressure IS 'Atmospheric pressure (hPa)';
COMMENT ON COLUMN weather_metrics.wind_speed IS 'Wind speed (m/s)';
COMMENT ON COLUMN weather_metrics.timestamp_unix IS 'Unix timestamp of the measurement from the API';
COMMENT ON COLUMN weather_metrics.created_at IS 'Date/time of insertion into the database';