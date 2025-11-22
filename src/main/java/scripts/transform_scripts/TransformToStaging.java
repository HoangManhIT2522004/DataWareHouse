package scripts.transform_scripts;

import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;
import org.w3c.dom.Element;

import java.sql.SQLException;

public class TransformToStaging {

    private static DBConn controlDB;
    private static DBConn stagingDB;

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("   WEATHER ETL - STEP 7 & 8: TRANSFORM & LOAD TO DW (IN STAGING DB)");
        System.out.println("========================================");

        String processName = "WA_Transform_Staging";
        String execId = "";

        try {
            // 1. Load Config
            LoadConfig config = new LoadConfig("config/config.xml");

            // 2. Kết nối DB
            controlDB = connectControlDB(config);
            stagingDB = connectStagingDB(config);

            // 3. Kiểm tra xem hôm nay đã chạy chưa
            if (checkTodayProcessSuccess(processName)) {
                System.out.println("⚠️ Hôm nay đã chạy Transform & Load thành công. Dừng tiến trình.");
                System.exit(0);
            }

            // 4. Tạo Log Process (Running)
            execId = "TR_" + System.currentTimeMillis();
            logProcess(execId, processName, "running", "Starting transformation and load...");

            // 5. Truncate bảng Staging
            truncateStagingTables();

            // ============================================================
            // A. TRANSFORM (RAW -> STAGING)
            // ============================================================
            System.out.println("\n--- A. TRANSFORM (RAW -> STAGING) ---");

            // 6. Transform: Location
            System.out.println(" [1/4] Transforming Locations...");
            int locCount = transformLocation();
            System.out.println("   -> Inserted " + locCount + " rows into stg_location.");

            // 7. Transform: Weather Condition
            System.out.println(" [2/4] Transforming Conditions...");
            int condCount = transformCondition();
            System.out.println("   -> Inserted " + condCount + " rows into stg_weather_condition.");

            // 8. Transform: Air Quality
            System.out.println(" [3/4] Transforming Air Quality...");
            int airCount = transformAirQuality();
            System.out.println("   -> Inserted " + airCount + " rows into stg_air_quality.");

            // 9. Transform: Weather Observation
            System.out.println(" [4/4] Transforming Observations...");
            int obsCount = transformObservation();
            System.out.println("   -> Inserted " + obsCount + " rows into stg_weather_observation.");

            int totalTransformedRows = locCount + condCount + airCount + obsCount;

            // ============================================================
            // B. LOAD (STAGING -> DATA WAREHOUSE)
            // ============================================================
            System.out.println("\n--- B. LOAD (STAGING -> DATA WAREHOUSE) ---");

            // 10. Load Dim_Location (SCD1)
            System.out.println(" [1/4] Loading Dim_Location (SCD1)...");
            int dimLocCount = loadDimLocation();
            System.out.println("   -> Dim_Location: Affected " + dimLocCount + " rows.");

            // 11. Load Dim_Weather_Condition (SCD1)
            System.out.println(" [2/4] Loading Dim_Weather_Condition (SCD1)...");
            int dimCondCount = loadDimWeatherCondition();
            System.out.println("   -> Dim_Condition: Affected " + dimCondCount + " rows.");

            // 12. Load Fact_Air_Quality (Snapshot Load)
            System.out.println(" [3/4] Loading Fact_Air_Quality...");
            int factAirCount = loadFactAirQualityDaily();
            System.out.println("   -> Fact_Air_Quality: Inserted " + factAirCount + " rows.");

            // 13. Load Fact_Weather_Observation (Snapshot Load)
            System.out.println(" [4/4] Loading Fact_Weather_Observation...");
            int factObsCount = loadFactWeatherDaily();
            System.out.println("   -> Fact_Weather_Observation: Inserted " + factObsCount + " rows.");

            int totalLoadedRows = dimLocCount + dimCondCount + factAirCount + factObsCount;

            // 14. Cập nhật Log (Success)
            String message = String.format("Completed. Transformed: %d rows. Loaded: %d rows.",
                    totalTransformedRows, totalLoadedRows);
            logProcessStatus(execId, "success", totalTransformedRows, message);

            System.out.println("\n========================================");
            System.out.println(" TRANSFORM AND LOAD PROCESS COMPLETED");
            System.out.println("========================================");

        } catch (Exception e) {
            System.err.println("\n TRANSFORM & LOAD PROCESS FAILED");
            e.printStackTrace();
            if (!execId.isEmpty()) {
                logProcessStatus(execId, "failed", 0, e.getMessage());
            }
            EmailSender.sendError("ETL Error: Transform & Load Failed", e.getMessage(), e);
            System.exit(1);
        }
    }

    // ============================================================
    // A. TRANSFORM METHODS (Giữ nguyên)
    // ============================================================

    private static int transformLocation() throws SQLException {
        String sql = "INSERT INTO stg_location " +
                "(location_id, city, region, country, lat, lon, tz_id, \"localtime\", \"localtime_epoch\", " +
                "record_status, hash_key, source_system, batch_id, raw_id, loaded_at) " +
                "SELECT " +
                "  MD5(UPPER(TRIM(name))), " +
                "  name, region, country, " +
                "  COALESCE(CAST(lat AS FLOAT), 0.0), COALESCE(CAST(lon AS FLOAT), 0.0), " +
                "  tz_id, " +
                "  TO_TIMESTAMP(CAST(\"localtime\" AS TEXT), 'YYYY-MM-DD HH24:MI'), " +
                "  CAST(\"localtime_epoch\" AS BIGINT), " +
                "  'pending', " +
                "  MD5(CONCAT(name, region, country, lat, lon, tz_id)), " +
                "  source_system, batch_id, id, NOW() " +
                "FROM raw_weather_location";
        return stagingDB.executeUpdate(sql);
    }

    private static int transformCondition() throws SQLException {
        String sql = "INSERT INTO stg_weather_condition " +
                "(condition_id, code, text, icon, record_status, hash_key, source_system, batch_id, raw_id, loaded_at) " +
                "SELECT " +
                "  code, " +
                "  CAST(code AS INT), " +
                "  MIN(text), " +
                "  MIN(icon), " +
                "  'pending', " +
                "  MD5(CONCAT(code, MIN(text), MIN(icon))), " +
                "  MIN(source_system), " +
                "  MIN(batch_id), " +
                "  MIN(id), " +
                "  NOW() " +
                "FROM raw_weather_condition " +
                "GROUP BY code";
        return stagingDB.executeUpdate(sql);
    }

    private static int transformAirQuality() throws SQLException {
        String sql = "INSERT INTO stg_air_quality " +
                "(aq_id, location_id, observation_time, co, no2, o3, so2, pm2_5, pm10, " +
                "us_epa_index, gb_defra_index, record_status, hash_key, source_system, batch_id, raw_id, loaded_at) " +
                "SELECT " +
                "  MD5(CONCAT(L.location_id, R.raw_payload->>'localtime')), " +
                "  L.location_id, " +
                "  TO_TIMESTAMP(R.raw_payload->>'localtime', 'YYYY-MM-DD HH24:MI'), " +
                "  CAST(R.co AS FLOAT), CAST(R.no2 AS FLOAT), CAST(R.o3 AS FLOAT), CAST(R.so2 AS FLOAT), " +
                "  CAST(R.pm2_5 AS FLOAT), CAST(R.pm10 AS FLOAT), " +
                "  CAST(R.us_epa_index AS INT), CAST(R.gb_defra_index AS INT), " +
                "  'pending', " +
                "  MD5(CONCAT(R.co, R.no2, R.o3, R.so2, R.pm2_5, R.pm10)), " +
                "  R.source_system, R.batch_id, R.id, NOW() " +
                "FROM raw_air_quality R " +
                "JOIN stg_location L ON L.city = (R.raw_payload->>'location_name') " +
                "WHERE L.batch_id = R.batch_id";
        return stagingDB.executeUpdate(sql);
    }

    private static int transformObservation() throws SQLException {
        String sql = "INSERT INTO stg_weather_observation " +
                "(observation_id, location_id, condition_id, observation_date, observation_time, " +
                "last_updated_epoch, temp_c, feelslike_c, pressure_mb, precip_mm, vis_km, wind_kph, gust_kph, " +
                "temp_f, feelslike_f, pressure_in, precip_in, vis_miles, wind_mph, gust_mph, " +
                "humidity_pct, wind_deg, wind_dir, cloud_pct, uv_index, is_day, " +
                "record_status, hash_key, source_system, batch_id, raw_id, loaded_at) " +
                "SELECT " +
                "  MD5(CONCAT(L.location_id, R.last_updated)), " +
                "  L.location_id, " +
                "  (R.raw_payload->>'condition_code'), " +
                "  COALESCE(CAST(CASE WHEN R.last_updated = '0' OR LENGTH(TRIM(R.last_updated)) < 16 THEN NULL ELSE R.last_updated END AS DATE), CURRENT_DATE), " +
                "  COALESCE(TO_TIMESTAMP(CASE WHEN R.last_updated = '0' OR LENGTH(TRIM(R.last_updated)) < 16 THEN NULL ELSE R.last_updated END, 'YYYY-MM-DD HH24:MI'), NOW()), " +
                "  COALESCE(CAST(NULLIF(R.last_updated_epoch, '0') AS BIGINT), 0), " +
                "  COALESCE(CAST(CASE WHEN R.temp_c ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.temp_c ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.feelslike_c ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.feelslike_c ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.pressure_mb ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.pressure_mb ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.precip_mm ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.precip_mm ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.vis_km ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.vis_km ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.wind_kph ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.wind_kph ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.gust_kph ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.gust_kph ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.temp_f ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.temp_f ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.feelslike_f ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.feelslike_f ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.pressure_in ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.pressure_in ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.precip_in ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.precip_in ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.vis_miles ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.vis_miles ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.wind_mph ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.wind_mph ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.gust_mph ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.gust_mph ELSE NULL END AS FLOAT), 0.0), " +
                "  COALESCE(CAST(CASE WHEN R.humidity ~ '^-?[0-9]+$' THEN R.humidity ELSE NULL END AS INT), 0), " +
                "  COALESCE(CAST(CASE WHEN R.wind_degree ~ '^-?[0-9]+$' THEN R.wind_degree ELSE NULL END AS INT), 0), " +
                "  R.wind_dir, " +
                "  COALESCE(CAST(CASE WHEN R.cloud ~ '^-?[0-9]+$' THEN R.cloud ELSE NULL END AS INT), 0), " +
                "  COALESCE(CAST(CASE WHEN R.uv ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.uv ELSE NULL END AS FLOAT), 0.0), " +
                "  (CASE WHEN R.is_day = '1' THEN TRUE ELSE FALSE END), " +
                "  'pending', " +
                "  MD5(CONCAT(R.temp_c, R.humidity, R.wind_kph, R.pressure_mb, R.precip_mm)), " +
                "  R.source_system, R.batch_id, R.id, NOW() " +
                "FROM raw_weather_observation R " +
                "JOIN stg_location L ON L.city = R.location_name " +
                "WHERE L.batch_id = R.batch_id";
        return stagingDB.executeUpdate(sql);
    }

    // ============================================================
    // B. LOAD DIMENSION METHODS (Giữ nguyên)
    // ============================================================

    private static int loadDimLocation() throws SQLException {
        // SCD Type 1: Insert/Update
        String sql = "INSERT INTO dim_location (" +
                "location_id, city, region, country, lat, lon, tz_id, \"localtime\", \"localtime_epoch\", " +
                "hash_key, loaded_at" +
                ") " +
                "SELECT " +
                "location_id, city, region, country, lat, lon, tz_id, \"localtime\", \"localtime_epoch\", " +
                "hash_key, NOW() " +
                "FROM public.stg_location " +
                "ON CONFLICT (location_id) DO UPDATE SET " +
                "    city = EXCLUDED.city, " +
                "    region = EXCLUDED.region, " +
                "    country = EXCLUDED.country, " +
                "    lat = EXCLUDED.lat, " +
                "    lon = EXCLUDED.lon, " +
                "    tz_id = EXCLUDED.tz_id, " +
                "    \"localtime\" = EXCLUDED.\"localtime\", " +
                "    \"localtime_epoch\" = EXCLUDED.\"localtime_epoch\", " +
                "    hash_key = EXCLUDED.hash_key, " +
                "    updated_at = NOW() " +
                "WHERE dim_location.hash_key IS DISTINCT FROM EXCLUDED.hash_key";
        return stagingDB.executeUpdate(sql);
    }

    private static int loadDimWeatherCondition() throws SQLException {
        // SCD Type 1: Insert/Update
        String sql = "INSERT INTO dim_weather_condition (" +
                "condition_id, code, text, icon, hash_key, loaded_at" +
                ") " +
                "SELECT " +
                "condition_id, code, text, icon, " +
                "hash_key, NOW() " +
                "FROM public.stg_weather_condition " +
                "ON CONFLICT (condition_id) DO UPDATE SET " +
                "    code = EXCLUDED.code, " +
                "    text = EXCLUDED.text, " +
                "    icon = EXCLUDED.icon, " +
                "    hash_key = EXCLUDED.hash_key, " +
                "    updated_at = NOW() " +
                "WHERE dim_weather_condition.hash_key IS DISTINCT FROM EXCLUDED.hash_key";
        return stagingDB.executeUpdate(sql);
    }

    // ============================================================
    // C. LOAD FACT METHODS
    // ============================================================

    private static int loadFactAirQualityDaily() throws SQLException {
        String sql = "INSERT INTO fact_air_quality_daily (" +
                "location_sk, date_sk, observation_time, " +
                "co, no2, o3, so2, pm2_5, pm10, us_epa_index, gb_defra_index, " +
                "source_system, batch_id, loaded_at" +
                ") " +
                "SELECT " +
                "    L_DIM.location_sk, " +
                "    D_DIM.date_sk, " +
                "    R.observation_time, " +
                "    R.co, R.no2, R.o3, R.so2, R.pm2_5, R.pm10, R.us_epa_index, R.gb_defra_index, " +
                "    R.source_system, R.batch_id, NOW() " +
                "FROM public.stg_air_quality R " +
                "JOIN dim_location L_DIM ON L_DIM.location_id = R.location_id " +
                "JOIN dim_date D_DIM ON D_DIM.full_date = DATE(R.observation_time) " +
                "WHERE R.record_status = 'pending' " +
                "ON CONFLICT (location_sk, observation_time) DO NOTHING";

        int inserted = stagingDB.executeUpdate(sql);

        String updateStagingSql = "UPDATE public.stg_air_quality SET record_status = 'loaded', loaded_at = NOW() WHERE record_status = 'pending'";
        stagingDB.executeUpdate(updateStagingSql);

        return inserted;
    }

    private static int loadFactWeatherDaily() throws SQLException {
        String sql = "INSERT INTO fact_weather_daily (" +
                "location_sk, condition_sk, date_sk, observation_date, observation_time, last_updated_epoch, " +
                "temp_c, feelslike_c, pressure_mb, precip_mm, vis_km, wind_kph, gust_kph, " +
                "temp_f, feelslike_f, pressure_in, precip_in, vis_miles, wind_mph, gust_mph, " +
                "humidity_pct, wind_deg, wind_dir, cloud_pct, uv_index, is_day, " +
                "source_system, batch_id, loaded_at" +
                ") " +
                "SELECT " +
                "    L_DIM.location_sk, " +
                "    COALESCE(C_DIM.condition_sk, (SELECT condition_sk FROM dim_weather_condition WHERE condition_id = '9999')), " +
                "    D_DIM.date_sk, " +
                "    R.observation_date, " +
                "    R.observation_time, " +
                "    R.last_updated_epoch, " +
                "    R.temp_c, R.feelslike_c, R.pressure_mb, R.precip_mm, R.vis_km, R.wind_kph, R.gust_kph, " +
                "    R.temp_f, R.feelslike_f, R.pressure_in, R.precip_in, R.vis_miles, R.wind_mph, R.gust_mph, " +
                "    R.humidity_pct, R.wind_deg, R.wind_dir, R.cloud_pct, R.uv_index, R.is_day, " +
                "    R.source_system, R.batch_id, NOW() " +
                "FROM public.stg_weather_observation R " +
                "JOIN dim_location L_DIM ON L_DIM.location_id = R.location_id " +
                "JOIN dim_date D_DIM ON D_DIM.full_date = R.observation_date " +
                "LEFT JOIN dim_weather_condition C_DIM ON C_DIM.condition_id = R.condition_id " +
                "WHERE R.record_status = 'pending' " +
                "ON CONFLICT (location_sk, observation_time) DO NOTHING";

        int inserted = stagingDB.executeUpdate(sql);

        // Cập nhật trạng thái staging
        String updateStagingSql = "UPDATE public.stg_weather_observation SET record_status = 'loaded', loaded_at = NOW() WHERE record_status = 'pending'";
        stagingDB.executeUpdate(updateStagingSql);

        return inserted;
    }

    private static void truncateStagingTables() throws SQLException {
        System.out.println("Cleaning old staging data...");
        // Truncate tất cả các bảng Staging
        String sql = "TRUNCATE TABLE stg_location, stg_weather_condition, stg_air_quality, stg_weather_observation";
        stagingDB.executeUpdate(sql);
    }

    // ============================================================
    // UTILS & LOGGING (Giữ nguyên)
    // ============================================================

    private static boolean checkTodayProcessSuccess(String processName) {
        final boolean[] exists = {false};
        String sql = "SELECT 1 FROM log_process WHERE config_process_id = " +
                "(SELECT config_process_id FROM config_process WHERE process_name = ?) " +
                "AND status = 'success' AND DATE(start_time) = CURRENT_DATE";
        try {
            controlDB.executeQuery(sql, rs -> {
                if (rs.next()) exists[0] = true;
            }, processName);
        } catch (Exception e) {
            System.err.println("⚠️ Warning: Check process status failed: " + e.getMessage());
        }
        return exists[0];
    }

    private static void logProcess(String execId, String processName, String status, String desc) {
        String sql = "INSERT INTO log_process (execution_id, config_process_id, start_time, status, error_message) " +
                "VALUES (?, (SELECT config_process_id FROM config_process WHERE process_name = ?), NOW(), ?::process_status, ?)";
        try {
            controlDB.executeUpdate(sql, execId, processName, status, desc);
        } catch (Exception e) {
            System.err.println("Warning: Log start failed: " + e.getMessage());
        }
    }

    private static void logProcessStatus(String execId, String status, int count, String desc) {
        String sql = "UPDATE log_process SET status = ?::process_status, end_time = NOW(), " +
                "records_inserted = ?, error_message = ? WHERE execution_id = ?";
        try {
            controlDB.executeUpdate(sql, status, count, desc, execId);
        } catch (Exception e) {
            System.err.println(" Warning: Log update failed: " + e.getMessage());
        }
    }

    private static DBConn connectControlDB(LoadConfig config) throws Exception {
        Element control = LoadConfig.getElement(config.getXmlDoc(), "control");
        return new DBConn(
                LoadConfig.getValue(control, "url"),
                LoadConfig.getValue(control, "username"),
                LoadConfig.getValue(control, "password")
        );
    }

    private static DBConn connectStagingDB(LoadConfig config) throws Exception {
        Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
        Element staging = LoadConfig.getChildElement(database, "staging");
        return new DBConn(
                LoadConfig.getValue(staging, "url"), LoadConfig.getValue(staging, "username"),
                LoadConfig.getValue(staging, "password")
        );
    }
}