package scripts.transform_scripts;

import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;
import org.w3c.dom.Element;

import java.sql.Connection;
import java.sql.SQLException;

public class TransformToStaging {

    private static DBConn controlDB;
    private static DBConn stagingDB;

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("   WEATHER ETL - STEP 7: TRANSFORM TO STAGING");
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
                System.out.println("️ Hôm nay đã chạy Transform thành công. Dừng tiến trình.");
                System.exit(0);
            }

            // 4. Tạo Log Process (Running)
            execId = "TR_" + System.currentTimeMillis();
            logProcess(execId, processName, "running", "Starting transformation...");

            // 5. Truncate bảng Staging (Làm sạch trước khi load mới)
            truncateStagingTables();

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

            // 10. Cập nhật Log (Success)
            int totalRows = locCount + condCount + airCount + obsCount;
            logProcessStatus(execId, "success", totalRows, "Transformation completed successfully");

            System.out.println("\n========================================");
            System.out.println(" TRANSFORM PROCESS COMPLETED");
            System.out.println("========================================");

        } catch (Exception e) {
            System.err.println("\n TRANSFORM PROCESS FAILED");
            e.printStackTrace();
            if (!execId.isEmpty()) {
                logProcessStatus(execId, "failed", 0, e.getMessage());
            }
            EmailSender.sendError("ETL Error: Transform To Staging Failed", e.getMessage(), e);
            System.exit(1);
        }
    }

    // ============================================================
    // TRANSFORM METHODS (SQL)
    // ============================================================

    private static int transformLocation() throws SQLException {
        // Fix: COALESCE for lat/lon, Quotes and CAST TEXT for localtime/localtime_epoch
        String sql = "INSERT INTO stg_location " +
                "(location_id, city, region, country, lat, lon, tz_id, \"localtime\", \"localtime_epoch\", " +
                "record_status, hash_key, source_system, batch_id, raw_id, loaded_at) " +
                "SELECT " +
                "  MD5(UPPER(TRIM(name))), " + // Generate Location ID
                "  name, region, country, " +
                "  COALESCE(CAST(lat AS FLOAT), 0.0), COALESCE(CAST(lon AS FLOAT), 0.0), " + // Fix NOT NULL violation
                "  tz_id, " +
                "  TO_TIMESTAMP(CAST(\"localtime\" AS TEXT), 'YYYY-MM-DD HH24:MI'), " + // Fix data type error
                "  CAST(\"localtime_epoch\" AS BIGINT), " +
                "  'pending', " +
                "  MD5(CONCAT(name, region, country, lat, lon, tz_id)), " + // Hash for change detection
                "  source_system, batch_id, id, NOW() " +
                "FROM raw_weather_location";
        return stagingDB.executeUpdate(sql);
    }

    private static int transformCondition() throws SQLException {
        // Fix Duplicate Key: Group only by the unique key (code)
        String sql = "INSERT INTO stg_weather_condition " +
                "(condition_id, code, text, icon, record_status, hash_key, source_system, batch_id, raw_id, loaded_at) " +
                "SELECT " +
                "  code, " + // This is condition_id
                "  CAST(code AS INT), " +
                "  MIN(text), " + // Aggregate descriptive fields
                "  MIN(icon), " + // Aggregate descriptive fields
                "  'pending', " +
                "  MD5(CONCAT(code, MIN(text), MIN(icon))), " + // Hash based on grouped values
                "  MIN(source_system), " + // Use MIN to arbitrarily select one value for metadata
                "  MIN(batch_id), " +
                "  MIN(id), " +
                "  NOW() " +
                "FROM raw_weather_condition " +
                "GROUP BY code"; // Group only by the unique key (code)
        return stagingDB.executeUpdate(sql);
    }

    private static int transformAirQuality() throws SQLException {
        String sql = "INSERT INTO stg_air_quality " +
                "(aq_id, location_id, observation_time, co, no2, o3, so2, pm2_5, pm10, " +
                "us_epa_index, gb_defra_index, record_status, hash_key, source_system, batch_id, raw_id, loaded_at) " +
                "SELECT " +
                "  MD5(CONCAT(L.location_id, R.raw_payload->>'localtime')), " + // Generate PK
                "  L.location_id, " +
                "  TO_TIMESTAMP(R.raw_payload->>'localtime', 'YYYY-MM-DD HH24:MI'), " +
                "  CAST(R.co AS FLOAT), CAST(R.no2 AS FLOAT), CAST(R.o3 AS FLOAT), CAST(R.so2 AS FLOAT), " +
                "  CAST(R.pm2_5 AS FLOAT), CAST(R.pm10 AS FLOAT), " +
                "  CAST(R.us_epa_index AS INT), CAST(R.gb_defra_index AS INT), " +
                "  'pending', " +
                "  MD5(CONCAT(R.co, R.no2, R.o3, R.so2, R.pm2_5, R.pm10)), " + // Hash data
                "  R.source_system, R.batch_id, R.id, NOW() " +
                "FROM raw_air_quality R " +
                "JOIN stg_location L ON L.city = (R.raw_payload->>'location_name') " +
                "WHERE L.batch_id = R.batch_id";
        return stagingDB.executeUpdate(sql);
    }

    private static int transformObservation() throws SQLException {
        // SỬA LỖI NOT NULL & INVALID INPUT: Thêm CASE WHEN để kiểm tra độ dài chuỗi R.last_updated
        // (cần ít nhất 16 ký tự cho định dạng 'YYYY-MM-DD HH:MI') trước khi cast, loại bỏ các giá trị rác như "24".
        String sql = "INSERT INTO stg_weather_observation " +
                "(observation_id, location_id, condition_id, observation_date, observation_time, " +
                "last_updated_epoch, temp_c, feelslike_c, pressure_mb, precip_mm, vis_km, wind_kph, gust_kph, " +
                "temp_f, feelslike_f, pressure_in, precip_in, vis_miles, wind_mph, gust_mph, " +
                "humidity_pct, wind_deg, wind_dir, cloud_pct, uv_index, is_day, " +
                "record_status, hash_key, source_system, batch_id, raw_id, loaded_at) " +
                "SELECT " +
                "  MD5(CONCAT(L.location_id, R.last_updated)), " + // Generate PK
                "  L.location_id, " +
                "  (R.raw_payload->>'condition_code'), " + // Condition ID

                // FIX: Dùng CASE để kiểm tra độ dài chuỗi (ít nhất 16 ký tự)
                "  COALESCE(CAST(CASE WHEN R.last_updated = '0' OR LENGTH(TRIM(R.last_updated)) < 16 THEN NULL ELSE R.last_updated END AS DATE), CURRENT_DATE), " +

                // FIX: Áp dụng kiểm tra độ dài tương tự cho TO_TIMESTAMP
                "  COALESCE(TO_TIMESTAMP(CASE WHEN R.last_updated = '0' OR LENGTH(TRIM(R.last_updated)) < 16 THEN NULL ELSE R.last_updated END, 'YYYY-MM-DD HH24:MI'), NOW()), " +

                // Fix cho last_updated_epoch
                "  COALESCE(CAST(NULLIF(R.last_updated_epoch, '0') AS BIGINT), 0), " +

                // Safe cast for all FLOAT columns (đã sửa ở bước trước)
                "  COALESCE(CAST(CASE WHEN R.temp_c ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.temp_c ELSE NULL END AS FLOAT), 0.0), " +          // temp_c
                "  COALESCE(CAST(CASE WHEN R.feelslike_c ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.feelslike_c ELSE NULL END AS FLOAT), 0.0), " +     // feelslike_c
                "  COALESCE(CAST(CASE WHEN R.pressure_mb ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.pressure_mb ELSE NULL END AS FLOAT), 0.0), " +     // pressure_mb
                "  COALESCE(CAST(CASE WHEN R.precip_mm ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.precip_mm ELSE NULL END AS FLOAT), 0.0), " +      // precip_mm
                "  COALESCE(CAST(CASE WHEN R.vis_km ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.vis_km ELSE NULL END AS FLOAT), 0.0), " +          // vis_km
                "  COALESCE(CAST(CASE WHEN R.wind_kph ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.wind_kph ELSE NULL END AS FLOAT), 0.0), " +        // wind_kph
                "  COALESCE(CAST(CASE WHEN R.gust_kph ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.gust_kph ELSE NULL END AS FLOAT), 0.0), " +        // gust_mph
                "  COALESCE(CAST(CASE WHEN R.temp_f ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.temp_f ELSE NULL END AS FLOAT), 0.0), " +          // temp_f
                "  COALESCE(CAST(CASE WHEN R.feelslike_f ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.feelslike_f ELSE NULL END AS FLOAT), 0.0), " +     // feelslike_f
                "  COALESCE(CAST(CASE WHEN R.pressure_in ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.pressure_in ELSE NULL END AS FLOAT), 0.0), " +     // pressure_in
                "  COALESCE(CAST(CASE WHEN R.precip_in ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.precip_in ELSE NULL END AS FLOAT), 0.0), " +       // precip_in
                "  COALESCE(CAST(CASE WHEN R.vis_miles ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.vis_miles ELSE NULL END AS FLOAT), 0.0), " +       // vis_miles
                "  COALESCE(CAST(CASE WHEN R.wind_mph ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.wind_mph ELSE NULL END AS FLOAT), 0.0), " +        // wind_mph
                "  COALESCE(CAST(CASE WHEN R.gust_mph ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.gust_mph ELSE NULL END AS FLOAT), 0.0), " +        // gust_mph

                // Safe cast for INT columns (đã sửa ở bước trước)
                "  COALESCE(CAST(CASE WHEN R.humidity ~ '^-?[0-9]+$' THEN R.humidity ELSE NULL END AS INT), 0), " +            // humidity_pct
                "  COALESCE(CAST(CASE WHEN R.wind_degree ~ '^-?[0-9]+$' THEN R.wind_degree ELSE NULL END AS INT), 0), " +         // wind_deg

                "  R.wind_dir, " +

                // Safe cast for remaining INT/FLOAT columns (đã sửa ở bước trước)
                "  COALESCE(CAST(CASE WHEN R.cloud ~ '^-?[0-9]+$' THEN R.cloud ELSE NULL END AS INT), 0), " +               // cloud_pct
                "  COALESCE(CAST(CASE WHEN R.uv ~ '^-?[0-9]+\\.?[0-9]*$' THEN R.uv ELSE NULL END AS FLOAT), 0.0), " +              // uv_index

                "  (CASE WHEN R.is_day = '1' THEN TRUE ELSE FALSE END), " +
                "  'pending', " +
                "  MD5(CONCAT(R.temp_c, R.humidity, R.wind_kph, R.pressure_mb, R.precip_mm)), " + // Hash critical metrics
                "  R.source_system, R.batch_id, R.id, NOW() " +
                "FROM raw_weather_observation R " +
                "JOIN stg_location L ON L.city = R.location_name " +
                "WHERE L.batch_id = R.batch_id";
        return stagingDB.executeUpdate(sql);
    }

    private static void truncateStagingTables() throws SQLException {
        System.out.println("🧹 Cleaning old staging data...");
        String sql = "TRUNCATE TABLE stg_location, stg_weather_condition, stg_air_quality, stg_weather_observation";
        stagingDB.executeUpdate(sql);
    }

    // ============================================================
    // UTILS & LOGGING
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
            System.err.println(" Warning: Check process status failed: " + e.getMessage());
        }
        return exists[0];
    }

    private static void logProcess(String execId, String processName, String status, String desc) {
        String sql = "INSERT INTO log_process (execution_id, config_process_id, start_time, status, error_message) " +
                "VALUES (?, (SELECT config_process_id FROM config_process WHERE process_name = ?), NOW(), ?::process_status, ?)";
        try {
            controlDB.executeUpdate(sql, execId, processName, status, desc);
        } catch (Exception e) {
            System.err.println("️ Warning: Log start failed: " + e.getMessage());
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