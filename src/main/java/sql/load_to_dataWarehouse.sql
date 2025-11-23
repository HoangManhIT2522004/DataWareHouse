-- 1. Bật dblink (chỉ chạy 1 lần)
-- dblink cho phép truy xuất dữ liệu từ một database khác trong PostgreSQL
CREATE EXTENSION IF NOT EXISTS dblink;

-- 2. Đảm bảo có khóa UNIQUE cho business key
-- Kiểm tra nếu chưa có UNIQUE constraint thì tạo để tránh trùng lặp dữ liệu
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1
                   FROM pg_constraint
                   WHERE conname = 'uk_dim_location'
                     AND conrelid = 'dim_location'::regclass) THEN
ALTER TABLE dim_location ADD CONSTRAINT uk_dim_location UNIQUE (location_id);
END IF;

    IF NOT EXISTS (SELECT 1
                   FROM pg_constraint
                   WHERE conname = 'uk_weather_condition'
                     AND conrelid = 'dim_weather_condition'::regclass) THEN
ALTER TABLE dim_weather_condition ADD CONSTRAINT uk_weather_condition UNIQUE (condition_id);
END IF;
END$$;

-- 3. DIM LOCATION – CỐ ĐỊNH VIỆT NAM
-- Hàm load_dim_location: lấy dữ liệu từ staging và insert/update vào dim_location
CREATE OR REPLACE FUNCTION load_dim_location()
RETURNS INTEGER AS $$
DECLARE
rec_count INTEGER := 0;  -- số bản ghi đã load
BEGIN
INSERT INTO dim_location (
    location_id, city, region, country, lat, lon, tz_id,
    "localtime", localtime_epoch, hash_key, updated_at
)
SELECT
    location_id,
    city,
    'Vietnam',                   -- cố định quốc gia là Việt Nam
    'Vietnam',
    lat::NUMERIC,
    lon::NUMERIC,
    'Asia/Ho_Chi_Minh',          -- timezone Việt Nam
    NOW(),                        -- thời gian hiện tại
    EXTRACT(EPOCH FROM NOW())::BIGINT,  -- thời gian epoch
    MD5(location_id || city),     -- hash key để kiểm tra thay đổi dữ liệu
    NOW()
FROM dblink(
             'dbname=staging user=postgres password=12345',
             'SELECT DISTINCT location_id, city, lat, lon
              FROM dim_location
              WHERE loaded_at >= CURRENT_DATE'
     ) AS t(location_id VARCHAR(100), city VARCHAR(100), lat TEXT, lon TEXT)
    ON CONFLICT (location_id) DO UPDATE SET
    city = EXCLUDED.city,
                                     lat = EXCLUDED.lat,
                                     lon = EXCLUDED.lon,
                                     hash_key = EXCLUDED.hash_key,
                                     updated_at = EXCLUDED.updated_at;

GET DIAGNOSTICS rec_count = ROW_COUNT;  -- lấy số bản ghi insert/update
RETURN rec_count;
END;
$$ LANGUAGE plpgsql;

-- 4. DIM WEATHER CONDITION
-- Hàm load_dim_weather_condition: lấy điều kiện thời tiết từ staging, insert/update
CREATE OR REPLACE FUNCTION load_dim_weather_condition()
RETURNS INTEGER AS $$
DECLARE rec_count INTEGER := 0;
BEGIN
INSERT INTO dim_weather_condition (condition_id, code, text, icon, hash_key, updated_at)
SELECT
    condition_id,
    code::INTEGER,
    text,
    icon,
    MD5(condition_id || code || text),  -- hash key kiểm tra thay đổi
    NOW()
FROM dblink(
             'dbname=staging user=postgres password=12345',
             'SELECT DISTINCT condition_id, code, text, icon
              FROM dim_weather_condition
              WHERE loaded_at >= CURRENT_DATE'
     ) AS t(condition_id VARCHAR(50), code TEXT, text TEXT, icon TEXT)
    ON CONFLICT (condition_id) DO UPDATE SET
    code = EXCLUDED.code,
                                      text = EXCLUDED.text,
                                      icon = EXCLUDED.icon,
                                      hash_key = EXCLUDED.hash_key,
                                      updated_at = EXCLUDED.updated_at;

GET DIAGNOSTICS rec_count = ROW_COUNT;
RETURN rec_count;
END;
$$ LANGUAGE plpgsql;

-- 5. FACT WEATHER DAILY – CHUẨN VIỆT NAM
-- Hàm load_fact_weather_daily: xóa dữ liệu hôm nay trước, sau đó insert mới từ staging
CREATE OR REPLACE FUNCTION load_fact_weather_daily()
RETURNS INTEGER AS $$
DECLARE rec_count INTEGER := 0;
BEGIN
    -- Xóa dữ liệu hiện có của ngày hôm nay để tránh trùng
DELETE FROM fact_weather_daily
WHERE observation_time::date = CURRENT_DATE;

-- Insert dữ liệu từ staging
INSERT INTO fact_weather_daily (
    location_sk, condition_sk, date_sk, observation_date, observation_time, last_updated_epoch,
    temp_c, feelslike_c, pressure_mb, precip_mm, vis_km, wind_kph, gust_kph,
    temp_f, feelslike_f, pressure_in, precip_in, vis_miles, wind_mph, gust_mph,
    humidity_pct, wind_deg, wind_dir, cloud_pct, uv_index, is_day,
    source_system, batch_id, loaded_at
)
SELECT
    l.location_sk, c.condition_sk, d.date_sk,
    f.observation_time::date, f.observation_time, f.last_updated_epoch,
    f.temp_c::NUMERIC, f.feelslike_c::NUMERIC,
    f.pressure_mb::NUMERIC, f.precip_mm::NUMERIC,
    f.vis_km::NUMERIC, f.wind_kph::NUMERIC, f.gust_kph::NUMERIC,
    f.temp_f::NUMERIC, f.feelslike_f::NUMERIC,
    f.pressure_in::NUMERIC, f.precip_in::NUMERIC,
    f.vis_miles::NUMERIC, f.wind_mph::NUMERIC, f.gust_mph::NUMERIC,
    f.humidity_pct::SMALLINT, f.wind_deg::SMALLINT, f.wind_dir,
    f.cloud_pct::SMALLINT, f.uv_index::NUMERIC,
    COALESCE(f.is_day ILIKE 'yes', FALSE),  -- chuyển 'yes' -> TRUE, else FALSE
    f.source_system, f.batch_id, NOW()
FROM dblink(
             'dbname=staging user=postgres password=12345',
             'SELECT
                 s.location_id, wc.condition_id,
                 f.observation_time, f.last_updated_epoch,
                 f.temp_c, f.feelslike_c, f.pressure_mb, f.precip_mm,
                 f.vis_km, f.wind_kph, f.gust_kph,
                 f.temp_f, f.feelslike_f, f.pressure_in, f.precip_in,
                 f.vis_miles, f.wind_mph, f.gust_mph,
                 f.humidity_pct, f.wind_deg, f.wind_dir,
                 f.cloud_pct, f.uv_index, f.is_day,
                 f.source_system, f.batch_id
              FROM fact_weather_daily f
              JOIN dim_location s ON f.location_sk = s.location_sk
              JOIN dim_weather_condition wc ON f.condition_sk = wc.condition_sk
              WHERE f.observation_time::date = CURRENT_DATE'
     ) AS f(
            location_id VARCHAR(100), condition_id VARCHAR(50),
            observation_time TIMESTAMP, last_updated_epoch BIGINT,
            temp_c TEXT, feelslike_c TEXT, pressure_mb TEXT, precip_mm TEXT,
            vis_km TEXT, wind_kph TEXT, gust_kph TEXT,
            temp_f TEXT, feelslike_f TEXT, pressure_in TEXT, precip_in TEXT,
            vis_miles TEXT, wind_mph TEXT, gust_mph TEXT,
            humidity_pct TEXT, wind_deg TEXT, wind_dir TEXT,
            cloud_pct TEXT, uv_index TEXT, is_day TEXT,
            source_system TEXT, batch_id TEXT
    )
         JOIN dim_location l          ON l.location_id = f.location_id
         JOIN dim_weather_condition c ON c.condition_id = f.condition_id
         JOIN dim_date d              ON d.full_date = f.observation_time::date;

GET DIAGNOSTICS rec_count = ROW_COUNT;
RETURN rec_count;
END;
$$ LANGUAGE plpgsql;

-- 6. FACT AQI – CHỈ VIỆT NAM
-- Hàm load_fact_air_quality_daily: xóa dữ liệu hôm nay và insert dữ liệu AQI
CREATE OR REPLACE FUNCTION load_fact_air_quality_daily()
RETURNS INTEGER AS $$
DECLARE rec_count INTEGER := 0;
BEGIN
DELETE FROM fact_air_quality_daily
WHERE observation_time::date = CURRENT_DATE;

INSERT INTO fact_air_quality_daily (
    location_sk, date_sk, observation_time, co, no2, o3, so2, pm2_5, pm10,
    us_epa_index, gb_defra_index, aqi_category, source_system, batch_id, loaded_at
)
SELECT
    l.location_sk, d.date_sk, a.observation_time,
    a.co::NUMERIC, a.no2::NUMERIC, a.o3::NUMERIC, a.so2::NUMERIC,
    a.pm2_5::NUMERIC, a.pm10::NUMERIC,
    a.us_epa_index::SMALLINT, a.gb_defra_index::SMALLINT,
    a.aqi_category, a.source_system, a.batch_id, NOW()
FROM dblink(
             'dbname=staging user=postgres password=12345',
             'SELECT s.location_id, a.observation_time, a.co, a.no2, a.o3, a.so2,
                     a.pm2_5, a.pm10, a.us_epa_index, a.gb_defra_index, a.aqi_category,
                     a.source_system, a.batch_id
              FROM fact_air_quality_daily a
              JOIN dim_location s ON a.location_sk = s.location_sk
              WHERE a.observation_time::date = CURRENT_DATE'
     ) AS a(
            location_id VARCHAR(100), observation_time TIMESTAMP,
            co TEXT, no2 TEXT, o3 TEXT, so2 TEXT, pm2_5 TEXT, pm10 TEXT,
            us_epa_index TEXT, gb_defra_index TEXT, aqi_category TEXT,
            source_system TEXT, batch_id TEXT
    )
         JOIN dim_location l ON l.location_id = a.location_id
         JOIN dim_date d     ON d.full_date = a.observation_time::date;

GET DIAGNOSTICS rec_count = ROW_COUNT;
RETURN rec_count;
END;
$$ LANGUAGE plpgsql;
