package scripts.transform_scripts;

import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;
import org.w3c.dom.Element;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TransformToStaging {

    private static DBConn controlDB;
    private static DBConn stagingDB;

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("   WEATHER ETL - STEP 7: TRANSFORM");
        System.out.println("========================================");

        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String configPath = args[0];
        String execId = null;

        try {
            System.out.println("[Config] Loading config from: " + configPath);
            LoadConfig config = new LoadConfig(configPath);
            controlDB = connectControlDB(config);
            stagingDB = connectStagingDB(config);

            checkTodayProcessSuccess();

            // Log process
            execId = prepareTransformProcess();

            // Thực hiện Transform
            int totalRows = transformData(execId);

            updateProcessLogStatus(execId, "success", totalRows, 0, "Transform Success");
            System.out.println("\nTRANSFORM COMPLETED SUCCESSFULLY");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("\nTRANSFORM FAILED");
            e.printStackTrace();
            if (execId != null) updateProcessLogStatus(execId, "failed", 0, 0, e.getMessage());
            EmailSender.sendError("ETL Error: Transform Failed", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static int transformData(String execId) throws Exception {
        System.out.println("[Process] Starting Data Transformation...");
        Connection conn = null;
        int totalUpdated = 0;

        try {
            conn = stagingDB.getConnection();
            conn.setAutoCommit(false);

            // --- PHASE 1: RAW TO STAGING ---
            System.out.println("\n--- [PHASE 1] RAW TO STAGING ---");

            // 1.1 Location
            totalUpdated += executeSQL(conn, "Staging Location",
                    "TRUNCATE TABLE stg_location",
                    "INSERT INTO stg_location (location_id, city, region, country, lat, lon, tz_id, \"localtime\", localtime_epoch, record_status, hash_key, source_system, batch_id) " +
                            "SELECT DISTINCT ON (r.name) r.name, r.name, r.region, r.country, CAST(r.lat AS float8), CAST(r.lon AS float8), " +

                            "   r.localtime AS tz_id, " +

                            "   CASE WHEN TRIM(r.localtime_epoch) ~ '^[0-9]+$' AND TRIM(r.localtime_epoch) != '' " +
                            "       THEN to_timestamp(CAST(TRIM(r.localtime_epoch) AS double precision)) " +
                            "       ELSE NULL " +
                            "   END AS \"localtime\", " +

                            "   CASE WHEN TRIM(r.localtime_epoch) ~ '^[0-9]+$' AND TRIM(r.localtime_epoch) != '' " +
                            "       THEN CAST(TRIM(r.localtime_epoch) AS BIGINT) " +
                            "       ELSE NULL " +
                            "   END AS localtime_epoch, " +

                            "   'pending', " +
                            "   MD5(CONCAT(r.name, r.region, r.country, r.lat, r.lon, r.localtime)), " +
                            "   r.source_system, r.batch_id " +
                            "FROM raw_weather_location r " +
                            "ORDER BY r.name, r.batch_id DESC");

            // 1.2 Condition
            totalUpdated += executeSQL(conn, "Staging Condition",
                    "TRUNCATE TABLE stg_weather_condition",
                    "INSERT INTO stg_weather_condition (condition_id, code, text, icon, record_status, hash_key, source_system, batch_id) " +
                            "SELECT DISTINCT ON (r.code) r.code, CAST(r.code AS int4), r.text, r.icon, 'pending', " +
                            "MD5(CONCAT(r.code, r.text, r.icon)), " +
                            "r.source_system, r.batch_id " +
                            "FROM raw_weather_condition r " +
                            "ORDER BY r.code, r.batch_id DESC");

            // 1.3 Observation
            totalUpdated += executeSQL(conn, "Staging Observation",
                    "TRUNCATE TABLE stg_weather_observation",
                    "INSERT INTO stg_weather_observation (" +
                            "   observation_id, location_id, condition_id, observation_date, observation_time, " +
                            "   is_day, " +
                            "   temp_c, feelslike_c, pressure_mb, precip_mm, humidity_pct, cloud_pct, uv_index, " +
                            "   vis_km, wind_kph, gust_kph, wind_deg, wind_dir, " +
                            "   record_status, hash_key, source_system, batch_id" +
                            ") " +
                            "SELECT DISTINCT ON (r.location_name, r.last_updated) " +
                            "   MD5(CONCAT(r.location_name, r.last_updated)), r.location_name, " +
                            "   (r.raw_payload->>'condition_code'), " +

                            "   CASE WHEN LENGTH(TRIM(r.last_updated)) < 10 THEN CAST('1900-01-01' AS date) ELSE CAST(r.last_updated AS date) END, " +
                            "   CASE WHEN LENGTH(TRIM(r.last_updated)) < 10 THEN CAST('1900-01-01 00:00:00' AS timestamp) ELSE CAST(r.last_updated AS timestamp) END, " +

                            "   CASE WHEN r.is_day IN ('0', '1') THEN CAST(r.is_day AS boolean) ELSE NULL END, " +

                            "   CAST(CASE WHEN r.temp_c ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN r.temp_c ELSE NULL END AS float8), " +
                            "   CAST(CASE WHEN r.feelslike_c ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN r.feelslike_c ELSE NULL END AS float8), " +
                            "   CAST(CASE WHEN r.pressure_mb ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN r.pressure_mb ELSE NULL END AS float8), " +
                            "   CAST(CASE WHEN r.precip_mm ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN r.precip_mm ELSE NULL END AS float8), " +

                            "   CAST(NULLIF(r.humidity, '') AS int2), " +
                            "   CAST(NULLIF(r.cloud, '') AS int2), " +

                            "   CAST(CASE WHEN r.uv ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN r.uv ELSE NULL END AS float8), " +

                            "   CAST(CASE WHEN r.vis_km ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN r.vis_km ELSE NULL END AS float8), " +

                            "   CAST(CASE WHEN r.wind_kph ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN r.wind_kph ELSE NULL END AS float8), " +
                            "   CAST(CASE WHEN r.gust_kph ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN r.gust_kph ELSE NULL END AS float8), " +

                            "   CAST(CASE WHEN r.wind_degree ~ '^[-+]?[0-9]+$' THEN r.wind_degree ELSE NULL END AS int4), " +

                            "   r.wind_dir, " +

                            "   'pending', " +
                            "   MD5(CONCAT(r.temp_c, r.humidity, r.precip_mm, r.uv, r.wind_kph, r.pressure_mb, r.vis_km, r.is_day)), " +
                            "   r.source_system, r.batch_id " +
                            "FROM raw_weather_observation r " +
                            "ORDER BY r.location_name, r.last_updated, r.batch_id DESC");

            // 1.4 Air Quality (FIX: Ép kiểu tường minh cho to_timestamp)
            totalUpdated += executeSQL(conn, "Staging Air Quality",
                    "TRUNCATE TABLE stg_air_quality",
                    "INSERT INTO stg_air_quality (aq_id, location_id, observation_time, co, no2, o3, so2, pm2_5, pm10, us_epa_index, gb_defra_index, record_status, hash_key, source_system, batch_id) " +
                            "SELECT DISTINCT ON (l.name, r.batch_id) " +
                            "MD5(CONCAT(l.name, r.batch_id)), l.name, " +
                            // FIX MỚI: Ép kiểu l.localtime_epoch sang double precision
                            "   COALESCE(to_timestamp(CAST(l.localtime_epoch AS double precision)), CAST('1900-01-01 00:00:00' AS timestamp)) AS observation_time, " +
                            "CAST(r.co AS float8), CAST(r.no2 AS float8), CAST(r.o3 AS float8), CAST(r.so2 AS float8), CAST(r.pm2_5 AS float8), CAST(r.pm10 AS float8), " +
                            "CAST(r.us_epa_index AS int4), CAST(r.gb_defra_index AS int4), 'pending', " +
                            "MD5(CONCAT(r.co, r.no2, r.pm2_5, r.pm10, r.us_epa_index)), " +
                            "r.source_system, r.batch_id " +
                            "FROM raw_air_quality r JOIN raw_weather_location l ON r.batch_id = l.batch_id " +
                            "ORDER BY l.name, r.batch_id DESC");


            // --- PHASE 2: STAGING TO DIM/FACT ---
            System.out.println("\n--- [PHASE 2] STAGING TO DIM/FACT ---");

            // 2.1 Dim Location
            totalUpdated += executeUpdate(conn, "Dim Location",
                    "INSERT INTO dim_location (location_id, city, region, country, lat, lon, tz_id, \"localtime\", localtime_epoch, hash_key, updated_at) " +
                            "SELECT location_id, city, region, country, lat, lon, tz_id, \"localtime\", localtime_epoch, hash_key, CURRENT_TIMESTAMP FROM stg_location " +
                            "ON CONFLICT (location_id) DO UPDATE SET " +
                            "   city = EXCLUDED.city, region = EXCLUDED.region, lat = EXCLUDED.lat, lon = EXCLUDED.lon, " +
                            "   \"localtime\" = EXCLUDED.\"localtime\", localtime_epoch = EXCLUDED.localtime_epoch, " +
                            "   hash_key = EXCLUDED.hash_key, updated_at = CURRENT_TIMESTAMP " +
                            "WHERE dim_location.hash_key IS DISTINCT FROM EXCLUDED.hash_key " +
                            "   OR dim_location.localtime_epoch IS DISTINCT FROM EXCLUDED.localtime_epoch");

            // 2.2 Dim Condition
            totalUpdated += executeUpdate(conn, "Dim Condition",
                    "INSERT INTO dim_weather_condition (condition_id, code, text, icon, hash_key, updated_at) " +
                            "SELECT condition_id, code, text, icon, hash_key, CURRENT_TIMESTAMP FROM stg_weather_condition " +
                            "ON CONFLICT (condition_id) DO UPDATE SET " +
                            "   text = EXCLUDED.text, icon = EXCLUDED.icon, hash_key = EXCLUDED.hash_key, updated_at = CURRENT_TIMESTAMP " +
                            "WHERE dim_weather_condition.hash_key IS DISTINCT FROM EXCLUDED.hash_key");

            // 2.3 Fact Weather Daily
            totalUpdated += executeUpdate(conn, "Fact Weather",
                    "INSERT INTO fact_weather_daily (" +
                            "   location_sk, condition_sk, date_sk, observation_date, observation_time, " +
                            "   is_day, " +
                            "   temp_c, humidity_pct, precip_mm, uv_index, " +
                            "   vis_km, wind_kph, gust_kph, wind_deg, wind_dir, " +
                            "   feelslike_c, pressure_mb, cloud_pct, batch_id, " +
                            "   source_system, loaded_at" +
                            ") " +
                            "SELECT dl.location_sk, dwc.condition_sk, dd.date_sk, s.observation_date, s.observation_time, " +
                            "   s.is_day, " +
                            "   s.temp_c, s.humidity_pct, s.precip_mm, s.uv_index, " +
                            "   s.vis_km, s.wind_kph, s.gust_kph, s.wind_deg, s.wind_dir, " +
                            "   s.feelslike_c, s.pressure_mb, s.cloud_pct, s.batch_id, " +
                            "   s.source_system, CURRENT_TIMESTAMP " +
                            "FROM stg_weather_observation s " +
                            "JOIN dim_location dl ON s.location_id = dl.location_id " +
                            "LEFT JOIN dim_weather_condition dwc ON s.condition_id = dwc.condition_id " +
                            "JOIN dim_date dd ON s.observation_date = dd.full_date " +
                            "ON CONFLICT (location_sk, observation_time) DO UPDATE SET " +
                            "   condition_sk = EXCLUDED.condition_sk, " +
                            "   temp_c = EXCLUDED.temp_c, humidity_pct = EXCLUDED.humidity_pct, precip_mm = EXCLUDED.precip_mm, uv_index = EXCLUDED.uv_index, " +
                            "   vis_km = EXCLUDED.vis_km, wind_kph = EXCLUDED.wind_kph, gust_kph = EXCLUDED.gust_kph, " +
                            "   feelslike_c = EXCLUDED.feelslike_c, pressure_mb = EXCLUDED.pressure_mb, cloud_pct = EXCLUDED.cloud_pct, " +
                            "   is_day = EXCLUDED.is_day, " +
                            "   batch_id = EXCLUDED.batch_id, loaded_at = CURRENT_TIMESTAMP " +
                            "WHERE fact_weather_daily.temp_c IS DISTINCT FROM EXCLUDED.temp_c " +
                            "   OR fact_weather_daily.condition_sk IS DISTINCT FROM EXCLUDED.condition_sk " +
                            "   OR fact_weather_daily.pressure_mb IS DISTINCT FROM EXCLUDED.pressure_mb");

            // 2.4 Fact Air Quality
            totalUpdated += executeUpdate(conn, "Fact Air Quality",
                    "INSERT INTO fact_air_quality_daily (location_sk, date_sk, observation_time, " +
                            "pm2_5, pm10, us_epa_index, co, no2, o3, so2, gb_defra_index, batch_id, source_system, loaded_at) " +
                            "SELECT dl.location_sk, dd.date_sk, s.observation_time, " +
                            "s.pm2_5, s.pm10, s.us_epa_index, s.co, s.no2, s.o3, s.so2, s.gb_defra_index, s.batch_id, s.source_system, CURRENT_TIMESTAMP " +
                            "FROM stg_air_quality s " +
                            "JOIN dim_location dl ON s.location_id = dl.location_id " +
                            "JOIN dim_date dd ON CAST(s.observation_time AS date) = dd.full_date " +
                            "ON CONFLICT (location_sk, observation_time) DO UPDATE SET " +
                            "   pm2_5 = EXCLUDED.pm2_5, us_epa_index = EXCLUDED.us_epa_index, " +
                            "   co = EXCLUDED.co, no2 = EXCLUDED.no2, o3 = EXCLUDED.o3, so2 = EXCLUDED.so2, gb_defra_index = EXCLUDED.gb_defra_index, " +
                            "   batch_id = EXCLUDED.batch_id, loaded_at = CURRENT_TIMESTAMP " +
                            "WHERE fact_air_quality_daily.pm2_5 IS DISTINCT FROM EXCLUDED.pm2_5 " +
                            "   OR fact_air_quality_daily.co IS DISTINCT FROM EXCLUDED.co");

            conn.commit();
            return totalUpdated;

        } catch (Exception e) {
            try { if(conn != null) conn.rollback(); } catch (SQLException ex) {}
            throw e;
        } finally {
            try { if(conn != null) conn.close(); } catch (SQLException ex) {}
        }
    }

    // --- Helper Methods ---
    private static int executeSQL(Connection conn, String name, String truncateSql, String insertSql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            System.out.print("  > " + name + "... ");
            if (truncateSql != null) stmt.execute(truncateSql);
            int rows = stmt.executeUpdate(insertSql);
            System.out.println("Inserted " + rows + " rows.");
            return rows;
        }
    }

    private static int executeUpdate(Connection conn, String name, String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            System.out.print("  > " + name + "... ");
            int rows = ps.executeUpdate();
            System.out.println("Upserted " + rows + " rows.");
            return rows;
        }
    }

    private static DBConn connectControlDB(LoadConfig config) throws Exception {
        Element control = LoadConfig.getElement(config.getXmlDoc(), "control");
        return new DBConn(LoadConfig.getValue(control, "url"), LoadConfig.getValue(control, "username"), LoadConfig.getValue(control, "password"));
    }

    private static DBConn connectStagingDB(LoadConfig config) throws Exception {
        Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
        Element staging = LoadConfig.getChildElement(database, "staging");
        return new DBConn(LoadConfig.getValue(staging, "url"), LoadConfig.getValue(staging, "username"), LoadConfig.getValue(staging, "password"));
    }

    private static void checkTodayProcessSuccess() {
        try {
            String sql = "SELECT check_today_transformstaging_success_prefix('TRF_STG_') AS success";
            final boolean[] alreadyLoaded = {false};
            controlDB.executeQuery(sql, rs -> { if (rs.next()) alreadyLoaded[0] = rs.getBoolean("success"); });
            if (alreadyLoaded[0]) { System.out.println("Hôm nay đã Transform thành công. Dừng tiến trình."); System.exit(0); }
        } catch (Exception e) { System.err.println("Warning Check Log: " + e.getMessage()); }
    }

    private static String prepareTransformProcess() throws Exception {
        System.out.println("[Prepare] Creating process log entry...");
        // Sử dụng process name dựa trên function create_new_transformstaging_log
        String processName = "TRF_STG_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String getConfigSql = String.format("SELECT get_or_create_config_transformstaging('%s', 'transform', 'staging_tables', 'staging', 'warehouse_tables')", processName);
        final int[] configId = {0};
        controlDB.executeQuery(getConfigSql, rs -> { if (rs.next()) configId[0] = rs.getInt(1); });
        if (configId[0] == 0) throw new Exception("Failed to get/create config_process ID");
        String createLogSql = "SELECT create_new_transformstaging_log(" + configId[0] + ")";
        final String[] execId = {null};
        controlDB.executeQuery(createLogSql, rs -> { if (rs.next()) execId[0] = rs.getString(1); });
        if (execId[0] == null) throw new Exception("Failed to create log_process entry");
        System.out.println("[Prepare] Created Execution ID: " + execId[0]);
        return execId[0];
    }

    private static void updateProcessLogStatus(String execId, String status, int inserted, int failed, String message) {
        try {
            String sql = "SELECT update_transformstaging_log_status(?, ?::process_status, ?, ?, ?)";
            controlDB.executeQuery(sql, rs -> {}, execId, status, inserted, failed, message);
            System.out.println("[Log] Updated status to: " + status);
        } catch (Exception e) { System.err.println("Failed update log: " + e.getMessage()); }
    }

    private static void printUsage() {
        System.out.println("\nUsage: java TransformToStaging <config_path>");
        System.out.println();
        System.out.println("Arguments:");
        System.out.println("  config_path : Path to main configuration file (e.g., config/config.xml)");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java TransformToStaging config/config.xml");
    }
}